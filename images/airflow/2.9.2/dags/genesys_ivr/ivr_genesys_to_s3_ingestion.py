import logging
import json
import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.secrets_manager import SecretsManagerHook
from datetime import datetime, timedelta
from snowflake import connector as sfconn
import pytz
import httpx
from time import sleep
import pendulum
import random
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# AWS S3 Configuration
S3_BUCKET_NAME = Variable.get("source_data_bucket") 
S3_DIRECTORY_PREFIX = 'genesys_raw'

# Genesys API Configuration
OAUTH_BASE_URL = 'https://login.usw2.pure.cloud/oauth/token'
API_BASE_URL = 'https://api.usw2.pure.cloud'
TOKEN_URL = OAUTH_BASE_URL
CONVERSATION_API_URL = f'{API_BASE_URL}/api/v2/analytics/conversations/details/query'
USER_AGGREGATE_API_URL = f'{API_BASE_URL}/api/v2/analytics/users/aggregates/query'
PARTICIPANT_API_URL = f'{API_BASE_URL}/api/v2/conversations/participants/attributes/search'
PAGE_SIZE = 100

snowflake_conn_id='scrt_conn_snowflake_conn_ai_dbt_user'

# Set the timezone for MST
local_tz = pendulum.timezone("America/Phoenix")

# API Rate Limit Configuration
RATE_LIMIT = 300  # 300 requests per minute
RATE_LIMIT_WINDOW = 60  # 60 seconds
REQUEST_TRACKING_WINDOW = 60  # Track requests in the last 60 seconds
MAX_RETRIES = 5
MIN_BACKOFF = 2  # Minimum backoff in seconds
MAX_BACKOFF = 120  # Maximum backoff in seconds
JITTER = 0.2  # Add random jitter to avoid thundering herd

# Request Tracking
request_timestamps = []

class GenesysRateLimitExceeded(Exception):
    """Exception raised when Genesys API rate limit is exceeded."""
    pass

class GenesysAuthError(Exception):
    """Exception raised when authentication with Genesys API fails."""
    pass

class GenesysAPIError(Exception):
    """Exception raised for Genesys API errors."""
    pass

def track_request():
    """
    Track API request timestamps and respect the rate limit.
    Returns delay time in seconds if rate limit is approaching.
    """
    global request_timestamps
    current_time = datetime.now().timestamp()
    
    # Clean up old timestamps
    request_timestamps = [ts for ts in request_timestamps 
                          if current_time - ts <= REQUEST_TRACKING_WINDOW]
    
    # Add current timestamp
    request_timestamps.append(current_time)
    
    # Check if approaching rate limit (80% of limit)
    recent_requests = len(request_timestamps)
    if recent_requests >= (RATE_LIMIT * 0.8):
        logging.warning(f"Approaching rate limit: {recent_requests}/{RATE_LIMIT} requests in the last minute")
        # Calculate delay time based on how close we are to the limit
        delay = (recent_requests / RATE_LIMIT) * (RATE_LIMIT_WINDOW / recent_requests)
        return max(delay, 1)  # Minimum delay of 1 second
    
    return 0  # No delay needed

@retry(
    retry=retry_if_exception_type((GenesysRateLimitExceeded, httpx.HTTPError, httpx.ConnectError)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=MIN_BACKOFF, max=MAX_BACKOFF),
    before_sleep=lambda retry_state: logging.warning(
        f"Retry attempt {retry_state.attempt_number} after {retry_state.outcome.exception()}, "
        f"waiting for {retry_state.next_action.sleep} seconds"
    )
)
def make_api_request(method, url, headers, json_data=None, params=None):
    """
    Make an API request with rate limiting and retry logic.
    
    Args:
        method: HTTP method ('get', 'post', etc.)
        url: API endpoint URL
        headers: HTTP headers
        json_data: JSON payload for POST requests
        params: URL parameters for GET requests
        
    Returns:
        API response as JSON
        
    Raises:
        GenesysRateLimitExceeded: When rate limit is exceeded
        GenesysAuthError: When authentication fails
        GenesysAPIError: For other API errors
    """
    # Check rate limit and delay if necessary
    delay = track_request()
    if (delay > 0):
        jitter = random.uniform(0, JITTER * delay)
        sleep_time = delay + jitter
        logging.info(f"Rate limit approaching, sleeping for {sleep_time:.2f} seconds")
        sleep(sleep_time)
    
    try:
        with httpx.Client(timeout=httpx.Timeout(60)) as client:
            if method.lower() == 'get':
                response = client.get(url, headers=headers, params=params)
            elif method.lower() == 'post':
                response = client.post(url, headers=headers, json=json_data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
        
        # Handle different status codes
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', MIN_BACKOFF))
            logging.warning(f"Rate limit exceeded. Retry after: {retry_after} seconds")
            raise GenesysRateLimitExceeded(f"Rate limit exceeded. Retry after: {retry_after} seconds")
        
        elif response.status_code == 401:
            logging.error("Authentication failed. Token may have expired.")
            raise GenesysAuthError("Authentication failed. Token may have expired.")
        
        elif response.status_code == 403:
            logging.error("Authorization failed. Insufficient permissions.")
            raise GenesysAuthError("Authorization failed. Insufficient permissions.")
        
        elif response.status_code >= 400:
            error_msg = f"API error: {response.status_code} - {response.text}"
            logging.error(error_msg)
            raise GenesysAPIError(error_msg)
        
        # Successfully received response
        response.raise_for_status()
        return response.json()
    
    except httpx.RequestError as e:
        logging.error(f"Request error: {e}")
        raise
    except httpx.HTTPStatusError as e:
        logging.error(f"HTTP error: {e}")
        raise

def get_date_range(**kwargs):
    """
    Get the date range for the past 'days' days.
    """
    days = int(Variable.get("date_range_days", default_var=1)) 
    today_mst = datetime.now(local_tz).date()
    target_mst = today_mst - timedelta(days=days)

    start_time_mst = target_mst.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_time_mst = today_mst.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    run_start_time = datetime.now(local_tz).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Push MST times to XCom
    kwargs['ti'].xcom_push(key='start_time_mst', value=start_time_mst)
    kwargs['ti'].xcom_push(key='end_time_mst', value=end_time_mst)
    kwargs['ti'].xcom_push(key='run_start_time', value=run_start_time)
    
    # Convert start and end of target_mst from MST to UTC
    start_utc = pendulum.datetime(target_mst.year, target_mst.month, target_mst.day, tz=local_tz).in_timezone('UTC')
    end_utc = pendulum.datetime(today_mst.year, today_mst.month, today_mst.day, tz=local_tz).in_timezone('UTC')
    
    return start_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"), end_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def get_secrets(**kwargs):
    """
    Retrieve the client_id and client_secret from AWS Secrets Manager.
    """
    try:
        logging.info("Attempting to retrieve secrets from AWS Secrets Manager")
        
        # Get the secret name from Airflow variable
        secret_name = Variable.get("genesys_ivr_secret_name", default_var="analytics-and-insights-airflow/genesys_ivr")
        
        hook = SecretsManagerHook()
        secret_value = hook.get_secret(secret_name)
        
        # Parse the JSON string into a dictionary
        secret_dict = json.loads(secret_value)
        
        # Retrieve the CLIENT_ID and CLIENT_SECRET
        client_id = secret_dict['CLIENT_ID']
        client_secret = secret_dict['CLIENT_SECRET']
        
        logging.info("Successfully retrieved secrets from AWS Secrets Manager")
        
        # Push the secrets to XCom without logging them
        kwargs['ti'].xcom_push(key='client_id', value=client_id)
        kwargs['ti'].xcom_push(key='client_secret', value=client_secret)
    except Exception as e:
        logging.error(f"Failed to retrieve secrets: {e}")
        raise

@retry(
    retry=retry_if_exception_type((httpx.HTTPError, httpx.ConnectError, httpx.TimeoutException, ConnectionError)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=MIN_BACKOFF, max=MAX_BACKOFF),
    before_sleep=lambda retry_state: logging.warning(
        f"OAuth token retrieval retry attempt {retry_state.attempt_number} after {retry_state.outcome.exception()}, "
        f"waiting for {retry_state.next_action.sleep} seconds"
    )
)
def get_oauth_token(**kwargs):
    """
    Retrieve an OAuth2 access token from the Genesys API.
    """
    logging.info("Pulling secrets from XCom...")
    client_id = kwargs['ti'].xcom_pull(task_ids='get_secrets', key='client_id')
    client_secret = kwargs['ti'].xcom_pull(task_ids='get_secrets', key='client_secret')
    if not client_id or not client_secret:
        logging.error("Failed to retrieve CLIENT_ID or CLIENT_SECRET from XCom.")
        raise ValueError("CLIENT_ID or CLIENT_SECRET is None")
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    
    logging.info("Retrieving OAuth2 token from Genesys...")
    try:
        with httpx.Client(timeout=httpx.Timeout(30)) as client:
            response = client.post(TOKEN_URL, headers=headers, data=data)
        response.raise_for_status()
        token = response.json().get('access_token')
        if not token:
            logging.error("Failed to retrieve OAuth token.")
            raise ValueError("OAuth token is None")
        logging.info("OAuth2 token acquired.")
        # Push token to XCom
        kwargs['ti'].xcom_push(key='oauth_token', value=token)
    except (httpx.HTTPError, httpx.ConnectError, httpx.TimeoutException) as e:
        logging.error(f"HTTP error obtaining OAuth token: {e}")
        raise  # Re-raise for retry
    except Exception as e:
        logging.error(f"Unexpected error obtaining OAuth token: {e}")
        raise

@retry(
    retry=retry_if_exception_type((boto3.exceptions.Boto3Error, ConnectionError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=lambda retry_state: logging.warning(
        f"S3 upload retry attempt {retry_state.attempt_number} after {retry_state.outcome.exception()}"
    )
)
def upload_to_s3(data, s3_key):
    """
    Upload the conversation data to an S3 bucket.
    """
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=json.dumps(data), Bucket=S3_BUCKET_NAME, Key=s3_key)
    logging.info(f"Data uploaded to S3 as {s3_key}.")

def log_s3_upload_details(s3_key, record_count, table_name, **kwargs):
    """
    Log the details of the S3 upload to Snowflake.
    """
    dag_id = kwargs['dag'].dag_id
    run_id = kwargs['run_id']
    upload_time = datetime.now(local_tz).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    status = 'SUCCESS'
    
    # Insert to s3_upload_log table

    log_sql = f"""
        INSERT INTO AI_DATAMART.AUDIT.s3_upload_log (
            log_id, 
            dag_id, 
            run_id, 
            table_name, 
            s3_key, 
            file_count, 
            upload_time, 
            status
        )
        VALUES (
            AI_DATAMART.AUDIT.s3_upload_log_seq.nextval,
            '{dag_id}',
            '{run_id}',
            '{table_name}',
            '{s3_key}',
            {record_count},
            '{upload_time}',
            '{status}'
        )
    """
    try:
        snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        snowflake_hook.run(log_sql)
        logging.info("Successfully logged S3 upload details to Snowflake.")
    except Exception as e:
        logging.error(f"Failed to log S3 upload details to Snowflake: {e}")
        raise  # Re-raise the exception to ensure the task fails

def get_conversation_data(**kwargs):
    """
    Retrieve conversation data from the Genesys API for the previous day.
    The data is then uploaded to an S3 bucket.
    """
       
    logging.info("Pulling date range from XCom...")
    utc_yesterday, utc_today = kwargs['ti'].xcom_pull(task_ids='get_date_range')
    if not utc_yesterday or not utc_today:
        logging.error("Failed to retrieve date range from XCom.")
        raise ValueError("Date range values are None.")
    
    logging.info(f"Pulled date range: {utc_yesterday} to {utc_today}")
    
    token = kwargs['ti'].xcom_pull(task_ids='get_oauth_token', key='oauth_token')
    if not token:
        logging.error("Failed to retrieve OAuth token from XCom.")
        raise ValueError("OAuth token is None")
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    page_number = 1
    total_conversations = 0
    run_date_time = datetime.now(pytz.utc).strftime('%Y-%m-%dT%H-%M-%S')
    s3_directory = f"{S3_DIRECTORY_PREFIX}/conversations/{run_date_time}"
    max_pages = 200  # Set a maximum limit for pages

    logging.info(f"Starting data retrieval for interval {utc_yesterday} to {utc_today}")

    # hard stopping to not pull more than 100 pages(10K conversation). Will need to change in future
    while page_number <= max_pages:
        logging.info(f'Retrieving page {page_number}')
        params = {
            'interval': f'{utc_yesterday}/{utc_today}',
            'paging': {
                'pageSize': PAGE_SIZE,
                'pageNumber': page_number
            },
            'conversationFilters': [
                {
                    'type': 'and',
                    'predicates': [
                        {
                            'type': 'dimension',
                            'dimension': 'conversationEnd',
                            'operator': 'exists'
                        }
                    ]
                }
            ]
        }
        
        try:
            # Use the new API request function with built-in retries
            data = make_api_request('post', CONVERSATION_API_URL, headers, params)
            conversations = data.get('conversations', [])
            page_conv_count = len(conversations)
            total_conversations += page_conv_count

            if conversations:
                s3_key = f"{s3_directory}/conversation_response_{page_number}.json"
                try:
                    upload_to_s3(conversations, s3_key)
                    log_s3_upload_details(s3_key, page_conv_count, 'IVR_CONVERSATIONS', **kwargs)
                except Exception as e:
                    logging.error(f"Failed to upload IVR Conversations to S3: {e}")
                    raise
            else:
                logging.info(f"No more conversations found. Total conversations retrieved: {total_conversations}")
                break
            
            # get total_hist(total conversation from API and calculate total pages
            total_hits = data.get('totalHits', 0)
            page_count = (total_hits + PAGE_SIZE - 1) // PAGE_SIZE  # Calculate total pages
            
            # break the loop if page number is greater than page count else go to next page
            if page_number >= page_count:
                logging.info(f"Data retrieval complete. Total conversations retrieved: {total_conversations}")
                break
            else:
                page_number += 1
                
        except GenesysRateLimitExceeded as e:
            logging.warning(f"Rate limit exceeded: {e}")
            # The retry decorator will handle this
            raise
        except (GenesysAuthError, GenesysAPIError) as e:
            logging.error(f"API error: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise

    if page_number > max_pages:
        logging.warning(f"Reached maximum page limit of {max_pages}. Data retrieval stopped.")
    
    logging.info(f"Total conversations retrieved: {total_conversations}")

    # Push total_conversations to XCom
    kwargs['ti'].xcom_push(key='total_conversations', value=total_conversations)

def get_user_aggregate_data(**kwargs):
    """
    Retrieve user aggregate data from the Genesys API for the previous day.
    The data is then uploaded to S3 bucket.
    """
    logging.info("Pulling date range from XCom...")
    utc_yesterday, utc_today = kwargs['ti'].xcom_pull(task_ids='get_date_range')
    if not utc_yesterday or not utc_today:
        logging.error("Failed to retrieve date range from XCom.")
        raise ValueError("Date range values are None.")
    
    token = kwargs['ti'].xcom_pull(task_ids='get_oauth_token', key='oauth_token')
    if not token:
        logging.error("Failed to retrieve OAuth token from XCom.")
        raise ValueError("OAuth token is None")
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    total_records = 0
    run_date_time = datetime.now(pytz.utc).strftime('%Y-%m-%dT%H-%M-%S')

    logging.info(f'Processing user aggregates data')
    
    params = {
        "interval": f"{utc_yesterday}/{utc_today}",
        "metrics": [
            "tSystemPresence",
            "tOrganizationPresence",
            "tAgentRoutingStatus"
        ],
        "filter": {
            "type": "or",
            "predicates": [
                {
                    "type": "dimension",
                    "dimension": "userId",
                    "operator": "matches",
                    "value": user_id
                }
                for user_id in [
                    "aadae03c-d04d-4f7c-a183-7a5abf873bb9",  # Amela Sabic
                    "f9e00831-b8dc-4228-b5a7-1cfc605d274e",  # Benjamin Solomona
                    "9bac9cb0-cc8c-4788-a91d-69f87b5124f0",  # Danielle Starnes
                    "4080f432-5e49-44fd-8a55-a9bcbedf005e",  # Ivan Ozuna Nieblas
                    "d4bf486b-3ccb-49a6-b918-f20b7e9db982",  # Karla Harris
                    "00d2b38a-06fa-4694-8fc8-28313829a785",  # Rebecca Starr
                    "1b69c5d7-a577-4827-ac65-50e09ef0dce6",  # Taylor Dunker
                    "4898e125-85ad-4439-bbae-29066453c16e",  # Blake Rosevear
                    "b8b7a98f-540b-4b53-a5a0-c76c71abe274",  # Siwei Ma
                    "f4fbc4bf-bf5a-4930-930c-eb6b4bbb2df2",  #"Drew Warner"
                    "68f11933-ce97-415f-b40c-3d3fc6a9f297",   # Hardik Shah
                    "08ba34ed-5669-493f-a945-bee6092db2b8", #Amanda Gailey
                    "b293dc39-c4c0-4de4-b322-65686a0f7dba" #Carla Recinos
                ]
            ]
        }
    }

    try:
        # Use the new API request function with built-in retries
        data = make_api_request('post', USER_AGGREGATE_API_URL, headers, params)
        results = data.get('results', [])
        page_user_count = len(results)
        total_records += page_user_count

        if results:
            s3_key = f"{S3_DIRECTORY_PREFIX}/user_aggregate/{run_date_time}/user_aggregate_data.json"
            try:
                upload_to_s3(results, s3_key)
                log_s3_upload_details(s3_key, page_user_count, 'IVR_USERS_AGGREGATES', **kwargs) # make same as table name
            except Exception as e:
                logging.error(f"Failed to upload IVR User aggregate to S3: {e}")
                raise
        else:
            logging.info(f"No user aggregate data found. Total records retrieved: {total_records}")

    except GenesysRateLimitExceeded as e:
        logging.warning(f"Rate limit exceeded: {e}")
        # The retry decorator will handle this
        raise
    except (GenesysAuthError, GenesysAPIError) as e:
        logging.error(f"API error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

    logging.info(f"Total user aggregate records retrieved: {total_records}")
    kwargs['ti'].xcom_push(key='total_user_records', value=total_records)

def get_participant_attributes(**kwargs):
    """
    Retrieve participant attributes from the Genesys API.
    The data is then uploaded to an S3 bucket.
    """
    logging.info("Pulling date range from XCom...")
    utc_yesterday, utc_today = kwargs['ti'].xcom_pull(task_ids='get_date_range')
    if not utc_yesterday or not utc_today:
        logging.error("Failed to retrieve date range from XCom.")
        raise ValueError("Date range values are None.")
    
    logging.info(f"Pulled date range: {utc_yesterday} to {utc_today}")
    
    token = kwargs['ti'].xcom_pull(task_ids='get_oauth_token', key='oauth_token')
    if not token:
        logging.error("Failed to retrieve OAuth token from XCom.")
        raise ValueError("OAuth token is None")
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    logging.info("Retrieving participant attributes from Genesys...")
    cursor = None
    run_date_time = datetime.now(pytz.utc).strftime('%Y-%m-%dT%H-%M-%S')
    s3_directory = f"{S3_DIRECTORY_PREFIX}/participant_attributes_search/{run_date_time}"
    page_number = 1
    total_count = 0

    while True:
        params = {
            "query": [
                {
                    "type": "DATE_RANGE",
                    "fields": ["endTime"],
                    "startValue": utc_yesterday,
                    "endValue": utc_today
                }
            ]
        }
        
        if cursor:
            params['cursor'] = cursor
        
        try:
            # Use the new API request function with built-in retries
            data = make_api_request('post', PARTICIPANT_API_URL, headers, params)
            page_results = data.get('results', [])
            
            page_count = len(page_results)
            total_count += page_count
            
            if page_results:
                s3_key = f"{s3_directory}/p_attributes_page_{page_number}.json"
                try:
                    upload_to_s3(page_results, s3_key)
                    log_s3_upload_details(s3_key, page_count, 'IVR_PARTICIPANT_ATTRIBUTES', **kwargs)
                except Exception as e:
                    logging.error(f"Failed to upload IVR Particiants to S3: {e}")
                    raise
                logging.info(f"Participant attributes data uploaded to S3 as {s3_key}.")
                logging.info(f"Page {page_number} processed with {page_count} records.")
                page_number += 1
            else:
                logging.info("No more participant attributes found.")
                break

            cursor = data.get('cursor')
            if not cursor:
                break
                
        except GenesysRateLimitExceeded as e:
            logging.warning(f"Rate limit exceeded: {e}")
            # The retry decorator will handle this
            raise
        except (GenesysAuthError, GenesysAPIError) as e:
            logging.error(f"API error: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise
    
    logging.info(f"Total pages processed: {page_number - 1}")
    logging.info(f"Total records retrieved: {total_count}")
    
    # Push results count to XCom
    kwargs['ti'].xcom_push(key='total_participant_attributes', value=total_count)

# Schedule change to 12:10am 
with DAG(
    'ivr_genesys_to_s3_ingestion',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,  # Increased from 1 to 2
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG to run Genesys API Ingestion Tasks',
    schedule_interval='10 7 * * *',  # 12:10am MST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['BI','DAILY_RUN','GENESYS'],
dagrun_timeout=timedelta(hours=1),  # Adding a 1-hour timeout for the entire DAG
) as dag:

    get_date_range_task = PythonOperator(
        task_id='get_date_range',
        python_callable=get_date_range,
        provide_context=True,
        do_xcom_push=True,
    )

    get_secrets_task = PythonOperator(
        task_id='get_secrets',
        python_callable=get_secrets,
        provide_context=True
    )

    get_oauth_token_task = PythonOperator(
        task_id='get_oauth_token',
        python_callable=get_oauth_token,
        provide_context=True
    )

    run_conversation_data_task = PythonOperator(
        task_id='run_conversation_data',
        python_callable=get_conversation_data,
        provide_context=True
    )

    mark_run_complete = SnowflakeOperator(
        task_id='mark_run_complete',
        sql="""
            INSERT INTO AI_DATAMART.AUDIT.api_ingestion_log (
                log_id, dag_id, run_id, start_ts, end_ts, status, 
                run_start_ts, run_end_ts, retry_count, 
                conversation_api_count,participation_api_count, user_api_count
            )
            VALUES (
                AI_DATAMART.AUDIT.api_ingestion_log_seq.nextval,
                '{{ dag.dag_id }}', 
                '{{ run_id }}', 
                '{{ ti.xcom_pull(task_ids='get_date_range', key='start_time_mst') }}', 
                '{{ ti.xcom_pull(task_ids='get_date_range', key='end_time_mst') }}', 
                'COMPLETED',
                '{{ ti.xcom_pull(task_ids='get_date_range', key='run_start_time') }}',
                CURRENT_TIMESTAMP(0), 
                {{ ti.try_number }}, 
                {{ ti.xcom_pull(task_ids='run_conversation_data', key='total_conversations') }},
                {{ ti.xcom_pull(task_ids='run_participant_attributes', key='total_participant_attributes') }},
                {{ ti.xcom_pull(task_ids='run_user_aggregate_data', key='total_user_records') }}
            )
        """,
        snowflake_conn_id=snowflake_conn_id,
        dag=dag,
    )

    run_user_aggregate_data_task = PythonOperator(
        task_id='run_user_aggregate_data',
        python_callable=get_user_aggregate_data,
        provide_context=True
    )

    run_participant_attributes_task = PythonOperator(
        task_id='run_participant_attributes',
        python_callable=get_participant_attributes,
        provide_context=True
    )

    get_date_range_task >> get_secrets_task >> get_oauth_token_task >> run_conversation_data_task >> run_user_aggregate_data_task >> run_participant_attributes_task >> mark_run_complete
