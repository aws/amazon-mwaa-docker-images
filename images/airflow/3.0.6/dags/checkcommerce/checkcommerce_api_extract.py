import json
import logging
import os
import requests
from datetime import datetime, date, timedelta
from textwrap import dedent
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowFailException
from pandas import DataFrame
from pendulum import datetime, now, duration
from snowflake.connector import SnowflakeConnection

from above.common.constants import (
    RAW_DATABASE_NAME,
    SNOWFLAKE_CONN_ID,
)
from above.common.slack_alert import task_failure_slack_alert
from above.common.snowflake_utils import dataframe_to_snowflake

logger: logging.Logger = logging.getLogger(__name__)
this_filename: str = str(os.path.basename(__file__).replace(".py", ""))
dag_start_date: datetime = datetime(2025, 6, 18, tz="UTC")


check_commerce_credentials: Dict = json.loads(Variable.get("checkcommerce"))
MERCHANT_NUMBER = check_commerce_credentials.get("merchant_number")
MERCHANT_PASSWORD = check_commerce_credentials.get("merchant_password")
AUTH_URL = check_commerce_credentials.get("auth_url")
REPORT_URL = check_commerce_credentials.get("report_url")

RAW_SCHEMA_NAME: str = "CHECK_COMMERCE"

snowflake_hook: SnowflakeHook = SnowflakeHook(SNOWFLAKE_CONN_ID)
snowflake_hook.database = RAW_DATABASE_NAME
snowflake_hook.schema = RAW_SCHEMA_NAME
snowflake_connection: SnowflakeConnection = snowflake_hook.get_conn()

PARAMS = {
    "UserName": MERCHANT_NUMBER,
    "Password": MERCHANT_PASSWORD,
    "Action": "Token",
    "OutputType": "JSON"
}

def get_token(AUTH_URL: str, PARAMS: dict) -> List:
    auth_response = requests.get(AUTH_URL, params=PARAMS)
    auth_data = auth_response.json()
    TOKEN = auth_data.get("Token")
    if not TOKEN:
        logger.error("Error getting token")
        raise AirflowFailException()
    return TOKEN

def make_api_request(REPORT_URL: str, report_params: dict) -> List:
    try:
        report_response = requests.get(REPORT_URL, params=report_params)
    except ValueError:
        logger.error("Invalid value")
        raise AirflowFailException()
    except Exception as e:
        logger.error(f"Other error: {e}")
        raise AirflowFailException()
    return report_response

def response_to_dataframe(report_response: List) -> DataFrame:
    try:
        report_data = report_response.json()
        df = DataFrame(report_data['ReportData']['Table0'])
    except ValueError:
        logger.error("Invalid value")
    except Exception as e:
        logger.error(f"Other error: {e}")
    return df

TOKEN = get_token(AUTH_URL, PARAMS)

@task
def get_and_load_merchant_list(MERCHANT_NUMBER: str, TOKEN: str, REPORT_URL: str, RAW_TABLE_NAME: str) -> None:
    """
    Fetches a dataframe of merchants from the Check Commerce API
    :return: None
    """

    report_params = {
        "Token": TOKEN,
        "Action": "Report",
        "OutputType": "JSON",
        "ReportPermDesc": "MerchantList",
        "Parameter_MID": MERCHANT_NUMBER
    }

    report_response = make_api_request(REPORT_URL, report_params)

    if report_response.status_code == 200:
        df = response_to_dataframe(report_response)

        if not df.empty:
            df['CREATED_AT'] = now("UTC")
            df.columns = df.columns.str.upper()
            suffix: str = "_UPDATES"
            dataframe_to_snowflake(
                df,
                database_name=RAW_DATABASE_NAME,
                schema_name=RAW_SCHEMA_NAME,
                table_name=f"{RAW_TABLE_NAME}{suffix}",
                overwrite=True,
                snowflake_connection=snowflake_connection,
            )
            merge_query: str = dedent(
                f"""
                merge into {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}
                    as target
                using {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}{suffix}
                    as source
                        on source.MID = target.MID
                when matched then update set
                    NAME = source.NAME
                    ,DBA = source.DBA
                    ,MAXSINGLEENTRYAMOUNT = source.MAXSINGLEENTRYAMOUNT
                    ,MAXDAILYAMOUNT = source.MAXDAILYAMOUNT
                    ,MAXDAILYTRANSACTIONS = source.MAXDAILYTRANSACTIONS
                    ,MAXMONTHLYAMOUNT = source.MAXMONTHLYAMOUNT
                    ,MAXMONTHLYTRANSACTIONS = source.MAXMONTHLYTRANSACTIONS
                    ,TRANFEE = source.TRANFEE
                    ,DISCFEE = source.DISCFEE
                    ,AUTHFEE = source.AUTHFEE
                    ,CHARGEBACKFEE = source.CHARGEBACKFEE
                    ,MONTHLYMAINTENANCEFEE = source.MONTHLYMAINTENANCEFEE
                    ,RETURNFEE = source.RETURNFEE
                    ,SETTLEMENTFEE = source.SETTLEMENTFEE
                    ,LASTSETTLEMENT = source.LASTSETTLEMENT
                    ,REPORTINGEMAIL = source.REPORTINGEMAIL
                    ,TERMINATIONDATE = source.TERMINATIONDATE
                    ,ORIGINATIONSUSPENDEDSTATUS = source.ORIGINATIONSUSPENDEDSTATUS
                    ,SETTLEMENTSUSPENDEDSTATUS = source.SETTLEMENTSUSPENDEDSTATUS
                    ,CREATED_AT = convert_timezone('UTC', source.CREATED_AT)::timestamp_ntz
                when not matched then insert (
                    MID
                    ,NAME
                    ,DBA
                    ,MAXSINGLEENTRYAMOUNT
                    ,MAXDAILYAMOUNT
                    ,MAXDAILYTRANSACTIONS
                    ,MAXMONTHLYAMOUNT
                    ,MAXMONTHLYTRANSACTIONS
                    ,TRANFEE
                    ,DISCFEE
                    ,AUTHFEE
                    ,CHARGEBACKFEE
                    ,MONTHLYMAINTENANCEFEE
                    ,RETURNFEE
                    ,SETTLEMENTFEE
                    ,LASTSETTLEMENT
                    ,REPORTINGEMAIL
                    ,TERMINATIONDATE
                    ,ORIGINATIONSUSPENDEDSTATUS
                    ,SETTLEMENTSUSPENDEDSTATUS
                    ,CREATED_AT
                ) values (
                    source.MID
                    ,source.NAME
                    ,source.DBA
                    ,source.MAXSINGLEENTRYAMOUNT
                    ,source.MAXDAILYAMOUNT
                    ,source.MAXDAILYTRANSACTIONS
                    ,source.MAXMONTHLYAMOUNT
                    ,source.MAXMONTHLYTRANSACTIONS
                    ,source.TRANFEE
                    ,source.DISCFEE
                    ,source.AUTHFEE
                    ,source.CHARGEBACKFEE
                    ,source.MONTHLYMAINTENANCEFEE
                    ,source.RETURNFEE
                    ,source.SETTLEMENTFEE
                    ,source.LASTSETTLEMENT
                    ,source.REPORTINGEMAIL
                    ,source.TERMINATIONDATE
                    ,source.ORIGINATIONSUSPENDEDSTATUS
                    ,source.SETTLEMENTSUSPENDEDSTATUS
                    ,source.CREATED_AT
                )
            """
            )

            try:
                snowflake_connection.cursor().execute(merge_query)
            except ValueError:
                logger.error("Invalid value")
                raise AirflowFailException()
            except Exception as e:
                logger.error(f"Other error: {e}")
                raise AirflowFailException()
    else:
        logger.info(report_response.text)

@task
def get_and_load_merchant_information(MERCHANT_NUMBER: str, TOKEN: str, REPORT_URL: str, RAW_TABLE_NAME: str)  -> None:
    """
    Fetches a dataframe of merchant information from the Check Commerce API
    :return: None
    """

    report_params = {
        "Token": TOKEN,
        "Action": "Report",
        "OutputType": "JSON",
        "ReportPermDesc": "MerchantInformation",
        "Parameter_MID": MERCHANT_NUMBER
    }

    report_response = make_api_request(REPORT_URL, report_params)
        
    if report_response.status_code == 200:
        df = response_to_dataframe(report_response)

        if not df.empty:
            df['CREATED_AT'] = now("UTC")
            df.columns = df.columns.str.upper()
            suffix: str = "_UPDATES"
            dataframe_to_snowflake(
                df,
                database_name=RAW_DATABASE_NAME,
                schema_name=RAW_SCHEMA_NAME,
                table_name=f"{RAW_TABLE_NAME}{suffix}",
                overwrite=True,
                snowflake_connection=snowflake_connection,
            )
            merge_query: str = dedent(
                f"""
                merge into {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}
                    as target
                using {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}{suffix}
                    as source
                        on source.COMP_NUM = target.COMP_NUM
                when matched then update set
                    COMP_NAME = source.COMP_NAME
                    ,SETTL_DAYS = source.SETTL_DAYS
                    ,CORP_BANK_NAME = source.CORP_BANK_NAME
                    ,CORP_BANK_ACC_NUM = source.CORP_BANK_ACC_NUM
                    ,CORP_BANK_ROUTING_NUM = source.CORP_BANK_ROUTING_NUM
                    ,CUTOFFTIME = source.CUTOFFTIME
                    ,PARENTMID = source.PARENTMID
                    ,PARENT = source.PARENT
                    ,CREATED_AT = convert_timezone('UTC', source.CREATED_AT)::timestamp_ntz
                when not matched then insert (
                    COMP_NUM
                    ,COMP_NAME
                    ,SETTL_DAYS
                    ,CORP_BANK_NAME
                    ,CORP_BANK_ACC_NUM
                    ,CORP_BANK_ROUTING_NUM
                    ,CUTOFFTIME
                    ,PARENTMID
                    ,PARENT
                    ,CREATED_AT
                ) values (
                    source.COMP_NUM
                    ,source.COMP_NAME
                    ,source.SETTL_DAYS
                    ,source.CORP_BANK_NAME
                    ,source.CORP_BANK_ACC_NUM
                    ,source.CORP_BANK_ROUTING_NUM
                    ,source.CUTOFFTIME
                    ,source.PARENTMID
                    ,source.PARENT
                    ,source.CREATED_AT
                )
            """
            )

            try:
                snowflake_connection.cursor().execute(merge_query)
            except ValueError:
                logger.error("Invalid value")
                raise AirflowFailException()
            except Exception as e:
                logger.error(f"Other error: {e}")
                raise AirflowFailException()
    else:
        logger.info(report_response.text)

@task
def get_and_load_invoice_aggregate_line_items(MERCHANT_NUMBER: str, TOKEN: str, REPORT_URL: str, RAW_TABLE_NAME: str) -> None:
    """
    Fetches a dataframe of invoice line items from the Check Commerce API
    :return: None
    """

    #The current month returns no data
    #This ensures we get the last month's data
    report_month = (date.today() + timedelta(-30)).strftime("%m")
    report_year = (date.today() + timedelta(-30)).strftime("%Y")

    report_params = {
        "Token": TOKEN,
        "Action": "Report",
        "OutputType": "JSON",
        "ReportPermDesc": "InvoiceAggregateLineItems",
        "Parameter_MID": MERCHANT_NUMBER,
        "Parameter_Year": report_year,
        "Parameter_Month": report_month
    }

    report_response = make_api_request(REPORT_URL, report_params)
        
    if report_response.status_code == 200:
        df = response_to_dataframe(report_response)

        if not df.empty:
            df['REPORT_MONTH'] = report_month
            df['REPORT_YEAR'] = report_year
            df['CREATED_AT'] = now("UTC")
            df.columns = df.columns.str.upper()
            #Sometimes the API returns duplicate data with the only difference being LIAMOUNT
            #One row has a value and one row has 0
            #This takes the max value and prevents duplicates
            df = df.sort_values(['COMPANY', 'LIDESCRIPTION', 'REPORT_MONTH', 'REPORT_YEAR', 'LIAMOUNT'], ascending=False)
            df = df.groupby(['COMPANY', 'LIDESCRIPTION', 'REPORT_MONTH', 'REPORT_YEAR']).first().reset_index()
            suffix: str = "_UPDATES"
            dataframe_to_snowflake(
                df,
                database_name=RAW_DATABASE_NAME,
                schema_name=RAW_SCHEMA_NAME,
                table_name=f"{RAW_TABLE_NAME}{suffix}",
                overwrite=True,
                snowflake_connection=snowflake_connection,
            )
            merge_query: str = dedent(
                f"""
                merge into {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}
                    as target
                using {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}{suffix}
                    as source
                        on source.COMPANY = target.COMPANY
                            and source.LIDESCRIPTION = target.LIDESCRIPTION
                            and source.REPORT_MONTH = target.REPORT_MONTH
                            and source.REPORT_YEAR = target.REPORT_YEAR
                when matched then update set
                    LICOUNT = source.LICOUNT
                    ,LIAMOUNT = source.LIAMOUNT
                    ,FEE = source.FEE
                    ,DISCOUNT = source.DISCOUNT
                    ,AMOUNT = source.AMOUNT
                    ,FEETYPEID = source.FEETYPEID
                    ,ISCHARGEBACK = source.ISCHARGEBACK
                    ,FEETYPEDETAIL = source.FEETYPEDETAIL
                    ,INVOICEBEGIN = source.INVOICEBEGIN
                    ,INVOICEEND = source.INVOICEEND
                    ,TRANSACTIONSTATUS = source.TRANSACTIONSTATUS
                    ,CREATED_AT = convert_timezone('UTC', source.CREATED_AT)::timestamp_ntz
                when not matched then insert (
                    COMPANY
                    ,LIDESCRIPTION
                    ,LICOUNT
                    ,LIAMOUNT
                    ,FEE
                    ,DISCOUNT
                    ,AMOUNT
                    ,FEETYPEID
                    ,ISCHARGEBACK
                    ,FEETYPEDETAIL
                    ,INVOICEBEGIN
                    ,INVOICEEND
                    ,TRANSACTIONSTATUS
                    ,REPORT_MONTH
                    ,REPORT_YEAR
                    ,CREATED_AT
                ) values (
                    source.COMPANY
                    ,source.LIDESCRIPTION
                    ,source.LICOUNT
                    ,source.LIAMOUNT
                    ,source.FEE
                    ,source.DISCOUNT
                    ,source.AMOUNT
                    ,source.FEETYPEID
                    ,source.ISCHARGEBACK
                    ,source.FEETYPEDETAIL
                    ,source.INVOICEBEGIN
                    ,source.INVOICEEND
                    ,source.TRANSACTIONSTATUS
                    ,source.REPORT_MONTH
                    ,source.REPORT_YEAR
                    ,source.CREATED_AT
                )
            """
            )

            try:
                snowflake_connection.cursor().execute(merge_query)
            except ValueError:
                logger.error("Invalid value")
                raise AirflowFailException()
            except Exception as e:
                logger.error(f"Other error: {e}")
                raise AirflowFailException()
    else:
        logger.info(report_response.text)

@task
def get_and_load_invoice_aggregate_demographic_data(MERCHANT_NUMBER: str, TOKEN: str, REPORT_URL: str, RAW_TABLE_NAME: str) -> None:
    """
    Fetches a dataframe of invoice aggregate demographic data from the Check Commerce API
    :return: None
    """

    report_month = date.today().strftime("%m")
    report_year = date.today().strftime("%Y")

    report_params = {
        "Token": TOKEN,
        "Action": "Report",
        "OutputType": "JSON",
        "ReportPermDesc": "InvoiceAggregateDemographicData",
        "Parameter_MID": MERCHANT_NUMBER,
        "Parameter_Year": report_year,
        "Parameter_Month": report_month
    }

    report_response = make_api_request(REPORT_URL, report_params)
        
    if report_response.status_code == 200:
        df = response_to_dataframe(report_response)

    if not df.empty:
        df['REPORT_MONTH'] = report_month
        df['REPORT_YEAR'] = report_year
        df['CREATED_AT'] = now("UTC")
        df.columns = df.columns.str.upper()
        suffix: str = "_UPDATES"
        dataframe_to_snowflake(
            df,
            database_name=RAW_DATABASE_NAME,
            schema_name=RAW_SCHEMA_NAME,
            table_name=f"{RAW_TABLE_NAME}{suffix}",
            overwrite=True,
            snowflake_connection=snowflake_connection,
        )
        merge_query: str = dedent(
            f"""
            merge into {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}
                as target
            using {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}{suffix}
                as source
                    on source.MID = target.MID
                        and source.REPORT_MONTH = target.REPORT_MONTH
                        and source.REPORT_YEAR = target.REPORT_YEAR                    
            when matched then update set
                COMP_NUM = source.COMP_NUM
                ,COMP_NAME = source.COMP_NAME
                ,MERCHANTNAME = source.MERCHANTNAME
                ,COMP_DISCRET = source.COMP_DISCRET
                ,COMPANYDISCRETIONARYDATA = source.COMPANYDISCRETIONARYDATA
                ,ADDRESS_1 = source.ADDRESS_1
                ,ADDRESS_2 = source.ADDRESS_2
                ,CITY = source.CITY
                ,STATE = source.STATE
                ,ZIP = source.ZIP
                ,ORIGINATION = source.ORIGINATION
                ,DISCOUNT = source.DISCOUNT
                ,MONTHLYMAINTENANCE = source.MONTHLYMAINTENANCE
                ,RETURN = source.RETURN
                ,NOC = source.NOC
                ,CHARGEBACK = source.CHARGEBACK
                ,AUTHORIZATION = source.AUTHORIZATION
                ,RETURNEDDISBURSEMENT = source.RETURNEDDISBURSEMENT
                ,POAINQUIRY = source.POAINQUIRY
                ,POAFAILURE = source.POAFAILURE
                ,OVERCHARGEBACKPERCENTAGEDISCOUNT = source.OVERCHARGEBACKPERCENTAGEDISCOUNT
                ,OVERRETURNPERCENTAGEDISCOUNT = source.OVERRETURNPERCENTAGEDISCOUNT
                ,OVERDAILYTHRESHOLDPENALTY = source.OVERDAILYTHRESHOLDPENALTY
                ,OVERMONTHLYTHRESHOLDPENALTY = source.OVERMONTHLYTHRESHOLDPENALTY
                ,MANUALINTERVENTION = source.MANUALINTERVENTION
                ,QUARTERLYCOMPLIANCEFEE = source.QUARTERLYCOMPLIANCEFEE
                ,MANUALREPORT = source.MANUALREPORT
                ,EXCESSIVESUPPORT = source.EXCESSIVESUPPORT
                ,CASHMANAGEMENT = source.CASHMANAGEMENT
                ,CASHMANAGEMENTDISCOUNT = source.CASHMANAGEMENTDISCOUNT
                ,OVERCHARGEBACKPENALTY = source.OVERCHARGEBACKPENALTY
                ,OVERRETURNPENALTY = source.OVERRETURNPENALTY
                ,SAMEDAYORIGINATION = source.SAMEDAYORIGINATION
                ,SAMEDAYDISCOUNT = source.SAMEDAYDISCOUNT
                ,APPLICATION = source.APPLICATION
                ,INVOICEBEGIN = source.INVOICEBEGIN
                ,INVOICEEND = source.INVOICEEND
                ,CREATED_AT = convert_timezone('UTC', source.CREATED_AT)::timestamp_ntz
            when not matched then insert (
                MID
                ,COMP_NUM
                ,COMP_NAME
                ,MERCHANTNAME
                ,COMP_DISCRET
                ,COMPANYDISCRETIONARYDATA
                ,ADDRESS_1
                ,ADDRESS_2
                ,CITY
                ,STATE
                ,ZIP
                ,ORIGINATION
                ,DISCOUNT
                ,MONTHLYMAINTENANCE
                ,RETURN
                ,NOC
                ,CHARGEBACK
                ,AUTHORIZATION
                ,RETURNEDDISBURSEMENT
                ,POAINQUIRY
                ,POAFAILURE
                ,OVERCHARGEBACKPERCENTAGEDISCOUNT
                ,OVERRETURNPERCENTAGEDISCOUNT
                ,OVERDAILYTHRESHOLDPENALTY
                ,OVERMONTHLYTHRESHOLDPENALTY
                ,MANUALINTERVENTION
                ,QUARTERLYCOMPLIANCEFEE
                ,MANUALREPORT
                ,EXCESSIVESUPPORT
                ,CASHMANAGEMENT
                ,CASHMANAGEMENTDISCOUNT
                ,OVERCHARGEBACKPENALTY
                ,OVERRETURNPENALTY
                ,SAMEDAYORIGINATION
                ,SAMEDAYDISCOUNT
                ,APPLICATION
                ,INVOICEBEGIN
                ,INVOICEEND
                ,REPORT_MONTH
                ,REPORT_YEAR
                ,CREATED_AT
            ) values (
                source.MID
                ,source.COMP_NUM
                ,source.COMP_NAME
                ,source.MERCHANTNAME
                ,source.COMP_DISCRET
                ,source.COMPANYDISCRETIONARYDATA
                ,source.ADDRESS_1
                ,source.ADDRESS_2
                ,source.CITY
                ,source.STATE
                ,source.ZIP
                ,source.ORIGINATION
                ,source.DISCOUNT
                ,source.MONTHLYMAINTENANCE
                ,source.RETURN
                ,source.NOC
                ,source.CHARGEBACK
                ,source.AUTHORIZATION
                ,source.RETURNEDDISBURSEMENT
                ,source.POAINQUIRY
                ,source.POAFAILURE
                ,source.OVERCHARGEBACKPERCENTAGEDISCOUNT
                ,source.OVERRETURNPERCENTAGEDISCOUNT
                ,source.OVERDAILYTHRESHOLDPENALTY
                ,source.OVERMONTHLYTHRESHOLDPENALTY
                ,source.MANUALINTERVENTION
                ,source.QUARTERLYCOMPLIANCEFEE
                ,source.MANUALREPORT
                ,source.EXCESSIVESUPPORT
                ,source.CASHMANAGEMENT
                ,source.CASHMANAGEMENTDISCOUNT
                ,source.OVERCHARGEBACKPENALTY
                ,source.OVERRETURNPENALTY
                ,source.SAMEDAYORIGINATION
                ,source.SAMEDAYDISCOUNT
                ,source.APPLICATION
                ,source.INVOICEBEGIN
                ,source.INVOICEEND
                ,source.REPORT_MONTH
                ,source.REPORT_YEAR
                ,source.CREATED_AT
            )
        """
        )

        try:
            snowflake_connection.cursor().execute(merge_query)
        except ValueError:
            logger.error("Invalid value")
            raise AirflowFailException()
        except Exception as e:
            logger.error(f"Other error: {e}")
            raise AirflowFailException()
    else:
        logger.info(report_response.text)            

@task
def get_and_load_aggregator_merchant_invoice(MERCHANT_NUMBER: str, TOKEN: str, REPORT_URL: str, RAW_TABLE_NAME: str) -> None:
    """
    Fetches a dataframe of aggregate merchant invoices from the Check Commerce API
    :return: None
    """

    #The current month returns no data
    #This ensures we get the last month's data
    report_month = (date.today() + timedelta(-30)).strftime("%m")
    report_year = (date.today() + timedelta(-30)).strftime("%Y")

    report_params = {
        "Token": TOKEN,
        "Action": "Report",
        "OutputType": "JSON",
        "ReportPermDesc": "AggregatorMerchantInvoice",
        "Parameter_AggregatorMid": MERCHANT_NUMBER,
        "Parameter_Month": report_month,
        "Parameter_Year": report_year
    }

    report_response = make_api_request(REPORT_URL, report_params)
        
    if report_response.status_code == 200:
        df = response_to_dataframe(report_response)

        if not df.empty:
            df.columns = df.columns.str.replace(' ', '_') 
            df['REPORT_MONTH'] = report_month
            df['REPORT_YEAR'] = report_year
            df['CREATED_AT'] = now("UTC")
            df.columns = df.columns.str.upper()
            #Sometimes the API returns duplicate data with the only difference being AMOUNT_PROCESSED
            #One row has a value and one row has 0
            #This takes the max value and prevents duplicates
            df = df.sort_values(['MERCHANT_NUMBER', 'INVOICE_START', 'INVOICE_END', 'DESCRIPTION', 'AMOUNT_PROCESSED'], ascending=False)
            df = df.groupby(['MERCHANT_NUMBER', 'INVOICE_START', 'INVOICE_END', 'DESCRIPTION']).first().reset_index()
            suffix: str = "_UPDATES"
            dataframe_to_snowflake(
                df,
                database_name=RAW_DATABASE_NAME,
                schema_name=RAW_SCHEMA_NAME,
                table_name=f"{RAW_TABLE_NAME}{suffix}",
                overwrite=True,
                snowflake_connection=snowflake_connection,
            )
            merge_query: str = dedent(
                f"""
                merge into {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}
                    as target
                using {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}{suffix}
                    as source
                        on source.MERCHANT_NUMBER = target.MERCHANT_NUMBER
                            and source.INVOICE_START = target.INVOICE_START
                            and source.INVOICE_END = target.INVOICE_END     
                            and source.DESCRIPTION = target.DESCRIPTION
                            and source.REPORT_MONTH = target.REPORT_MONTH
                            and source.REPORT_YEAR = target.REPORT_YEAR
                when matched then update set
                    COMPANY = source.COMPANY
                    ,FEE = source.FEE
                    ,COUNT = source.COUNT
                    ,AMOUNT_PROCESSED = source.AMOUNT_PROCESSED
                    ,DISCOUNT = source.DISCOUNT
                    ,AMOUNT_OWED = source.AMOUNT_OWED
                    ,REPORT_MONTH = source.REPORT_MONTH
                    ,REPORT_YEAR = source.REPORT_YEAR
                    ,CREATED_AT = convert_timezone('UTC', source.CREATED_AT)::timestamp_ntz
                when not matched then insert (
                    INVOICE_START
                    ,INVOICE_END
                    ,MERCHANT_NUMBER
                    ,COMPANY
                    ,DESCRIPTION
                    ,FEE
                    ,COUNT
                    ,AMOUNT_PROCESSED
                    ,DISCOUNT
                    ,AMOUNT_OWED
                    ,REPORT_MONTH
                    ,REPORT_YEAR
                    ,CREATED_AT
                ) values (
                    source.INVOICE_START
                    ,source.INVOICE_END
                    ,source.MERCHANT_NUMBER
                    ,source.COMPANY
                    ,source.DESCRIPTION
                    ,source.FEE
                    ,source.COUNT
                    ,source.AMOUNT_PROCESSED
                    ,source.DISCOUNT
                    ,source.AMOUNT_OWED
                    ,source.REPORT_MONTH
                    ,source.REPORT_YEAR
                    ,source.CREATED_AT
                )
            """
            )

            try:
                snowflake_connection.cursor().execute(merge_query)
            except ValueError:
                logger.error("Invalid value")
                raise AirflowFailException()
            except Exception as e:
                logger.error(f"Other error: {e}")
                raise AirflowFailException()
    else:
        logger.info(report_response.text)                

@task
def get_and_load_transaction_log(MERCHANT_NUMBER: str, TOKEN: str, REPORT_URL: str, RAW_TABLE_NAME: str) -> None:
    """
    Fetches a dataframe of transaction data from the Check Commerce API
    :return: None
    """

    end_date = date.today()
    start_date = end_date - timedelta(days=1)

    start_date_string = start_date.strftime("%Y-%m-%d")
    end_date_string = end_date.strftime("%Y-%m-%d")

    report_params = {
        "Token": TOKEN,
        "Action": "Report",
        "OutputType": "JSON",
        "ReportPermDesc": "TransactionLog",
        "Parameter_CoNo": MERCHANT_NUMBER,
        "Parameter_DateProcessStart": start_date_string,
        "Parameter_DateProcessEnd": end_date_string
    }

    report_response = make_api_request(REPORT_URL, report_params)
        
    if report_response.status_code == 200:
        df = response_to_dataframe(report_response)

        if not df.empty:
            df['START_DATE'] = start_date_string
            df['END_DATE'] = end_date_string
            df['CREATED_AT'] = now("UTC")
            df.columns = df.columns.str.upper()
            suffix: str = "_UPDATES"
            dataframe_to_snowflake(
                df,
                database_name=RAW_DATABASE_NAME,
                schema_name=RAW_SCHEMA_NAME,
                table_name=f"{RAW_TABLE_NAME}{suffix}",
                overwrite=True,
                snowflake_connection=snowflake_connection,
            )
            merge_query: str = dedent(
                f"""
                merge into {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}
                    as target
                using {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}{suffix}
                    as source
                        on source.ID = target.ID
                            and source.START_DATE = target.START_DATE
                            and source.END_DATE = target.END_DATE
                when matched then update set
                    OURREFERENCENUMBER = source.OURREFERENCENUMBER
                    ,THEIRREFERENCENUMBER = source.THEIRREFERENCENUMBER
                    ,APPROVALCODE = source.APPROVALCODE
                    ,DATEPROCESSED = source.DATEPROCESSED
                    ,EFFECTIVEENTRYDATE = source.EFFECTIVEENTRYDATE
                    ,NAME = source.NAME
                    ,PHONENUMBER = source.PHONENUMBER
                    ,INDIVIDUALID = source.INDIVIDUALID
                    ,AMOUNT = source.AMOUNT
                    ,RETURNDATE = source.RETURNDATE
                    ,RETURNREASONCODE = source.RETURNREASONCODE
                    ,RESUBMITTED = source.RESUBMITTED
                    ,RESUBMITTEDREFERENCENUMBER = source.RESUBMITTEDREFERENCENUMBER
                    ,DATESETTLED = source.DATESETTLED
                    ,BANKROUTINGNUMBER = source.BANKROUTINGNUMBER
                    ,CHECKNUMBER = source.CHECKNUMBER
                    ,AMOUNTDISBURSED = source.AMOUNTDISBURSED
                    ,STATUS = source.STATUS
                    ,NOTE = source.NOTE
                    ,DLN = source.DLN
                    ,SSN = source.SSN
                    ,ADDRESS1 = source.ADDRESS1
                    ,ADDRESS2 = source.ADDRESS2
                    ,CITY = source.CITY
                    ,STATE = source.STATE
                    ,ZIP = source.ZIP
                    ,CUSTOMEREMAIL = source.CUSTOMEREMAIL
                    ,BIRTHDATE = source.BIRTHDATE
                    ,DLNISSUESTATE = source.DLNISSUESTATE
                    ,NOCDISBURSED = source.NOCDISBURSED
                    ,NOCINFODATA = source.NOCINFODATA
                    ,NOCDATE = source.NOCDATE
                    ,ISLIVE = source.ISLIVE
                    ,DATELASTUPDATED = source.DATELASTUPDATED
                    ,BANKACCOUNTNOMASKED = source.BANKACCOUNTNOMASKED
                    ,ORIGINATEDATE = source.ORIGINATEDATE
                    ,REFUNDS = source.REFUNDS
                    ,REFUNDEDID = source.REFUNDEDID
                    ,SECCODE = source.SECCODE
                    ,DESCRIPTOR = source.DESCRIPTOR
                    ,WAREHOUSEID = source.WAREHOUSEID
                    ,BATCHUPLOADID = source.BATCHUPLOADID
                    ,AUTHID = source.AUTHID
                    ,MERCHANTNUMBER = source.MERCHANTNUMBER
                    ,CUSTREFID = source.CUSTREFID
                    ,START_DATE = source.START_DATE
                    ,END_DATE = source.END_DATE
                    ,CREATED_AT = convert_timezone('UTC', source.CREATED_AT)::timestamp_ntz
                when not matched then insert (
                    ID
                    ,OURREFERENCENUMBER
                    ,THEIRREFERENCENUMBER
                    ,APPROVALCODE
                    ,DATEPROCESSED
                    ,EFFECTIVEENTRYDATE
                    ,NAME
                    ,PHONENUMBER
                    ,INDIVIDUALID
                    ,AMOUNT
                    ,RETURNDATE
                    ,RETURNREASONCODE
                    ,RESUBMITTED
                    ,RESUBMITTEDREFERENCENUMBER
                    ,DATESETTLED
                    ,BANKROUTINGNUMBER
                    ,CHECKNUMBER
                    ,AMOUNTDISBURSED
                    ,STATUS
                    ,NOTE
                    ,DLN
                    ,SSN
                    ,ADDRESS1
                    ,ADDRESS2
                    ,CITY
                    ,STATE
                    ,ZIP
                    ,CUSTOMEREMAIL
                    ,BIRTHDATE
                    ,DLNISSUESTATE
                    ,NOCDISBURSED
                    ,NOCINFODATA
                    ,NOCDATE
                    ,ISLIVE
                    ,DATELASTUPDATED
                    ,BANKACCOUNTNOMASKED
                    ,ORIGINATEDATE
                    ,REFUNDS
                    ,REFUNDEDID
                    ,SECCODE
                    ,DESCRIPTOR
                    ,WAREHOUSEID
                    ,BATCHUPLOADID
                    ,AUTHID
                    ,MERCHANTNUMBER
                    ,CUSTREFID
                    ,START_DATE
                    ,END_DATE
                    ,CREATED_AT
                ) values (
                    source.ID
                    ,source.OURREFERENCENUMBER
                    ,source.THEIRREFERENCENUMBER
                    ,source.APPROVALCODE
                    ,source.DATEPROCESSED
                    ,source.EFFECTIVEENTRYDATE
                    ,source.NAME
                    ,source.PHONENUMBER
                    ,source.INDIVIDUALID
                    ,source.AMOUNT
                    ,source.RETURNDATE
                    ,source.RETURNREASONCODE
                    ,source.RESUBMITTED
                    ,source.RESUBMITTEDREFERENCENUMBER
                    ,source.DATESETTLED
                    ,source.BANKROUTINGNUMBER
                    ,source.CHECKNUMBER
                    ,source.AMOUNTDISBURSED
                    ,source.STATUS
                    ,source.NOTE
                    ,source.DLN
                    ,source.SSN
                    ,source.ADDRESS1
                    ,source.ADDRESS2
                    ,source.CITY
                    ,source.STATE
                    ,source.ZIP
                    ,source.CUSTOMEREMAIL
                    ,source.BIRTHDATE
                    ,source.DLNISSUESTATE
                    ,source.NOCDISBURSED
                    ,source.NOCINFODATA
                    ,source.NOCDATE
                    ,source.ISLIVE
                    ,source.DATELASTUPDATED
                    ,source.BANKACCOUNTNOMASKED
                    ,source.ORIGINATEDATE
                    ,source.REFUNDS
                    ,source.REFUNDEDID
                    ,source.SECCODE
                    ,source.DESCRIPTOR
                    ,source.WAREHOUSEID
                    ,source.BATCHUPLOADID
                    ,source.AUTHID
                    ,source.MERCHANTNUMBER
                    ,source.CUSTREFID
                    ,source.START_DATE
                    ,source.END_DATE
                    ,source.CREATED_AT
                )
            """
            )

            try:
                snowflake_connection.cursor().execute(merge_query)
            except ValueError:
                logger.error("Invalid value")
                raise AirflowFailException()
            except Exception as e:
                logger.error(f"Other error: {e}")
                raise AirflowFailException()
    else:
        logger.info(report_response.text)                

@task
def get_and_load_returned_transactions(MERCHANT_NUMBER: str, TOKEN: str, REPORT_URL: str, RAW_TABLE_NAME: str) -> None:
    """
    Fetches a dataframe of returned transactions from the Check Commerce API and loads them to Snowflake
    :return: None
    """

    end_date = date.today()
    start_date = end_date - timedelta(days=1)

    start_date_string = start_date.strftime("%Y-%m-%d")
    end_date_string = end_date.strftime("%Y-%m-%d")

    report_params = {
        "Token": TOKEN,
        "Action": "Report",
        "OutputType": "JSON",
        "ReportPermDesc": "TransactionLog",
        "Parameter_CoNo": MERCHANT_NUMBER,
        "Parameter_ReturnDateStart": start_date_string,
        "Parameter_ReturnDateEnd": end_date_string
    }

    report_response = make_api_request(REPORT_URL, report_params)
        
    if report_response.status_code == 200:
        df = response_to_dataframe(report_response)

        if not df.empty:
            df['START_DATE'] = start_date_string
            df['END_DATE'] = end_date_string
            df['CREATED_AT'] = now("UTC")
            df.columns = df.columns.str.upper()
            suffix: str = "_UPDATES"
            dataframe_to_snowflake(
                df,
                database_name=RAW_DATABASE_NAME,
                schema_name=RAW_SCHEMA_NAME,
                table_name=f"{RAW_TABLE_NAME}{suffix}",
                overwrite=True,
                snowflake_connection=snowflake_connection,
            )
            merge_query: str = dedent(
                f"""
                merge into {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}
                    as target
                using {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}{suffix}
                    as source
                        on source.ID = target.ID
                            and source.START_DATE = target.START_DATE
                            and source.END_DATE = target.END_DATE                        
                when matched then update set
                    OURREFERENCENUMBER = source.OURREFERENCENUMBER
                    ,THEIRREFERENCENUMBER = source.THEIRREFERENCENUMBER
                    ,APPROVALCODE = source.APPROVALCODE
                    ,DATEPROCESSED = source.DATEPROCESSED
                    ,EFFECTIVEENTRYDATE = source.EFFECTIVEENTRYDATE
                    ,NAME = source.NAME
                    ,PHONENUMBER = source.PHONENUMBER
                    ,INDIVIDUALID = source.INDIVIDUALID
                    ,AMOUNT = source.AMOUNT
                    ,RETURNDATE = source.RETURNDATE
                    ,RETURNREASONCODE = source.RETURNREASONCODE
                    ,RESUBMITTED = source.RESUBMITTED
                    ,RESUBMITTEDREFERENCENUMBER = source.RESUBMITTEDREFERENCENUMBER
                    ,DATESETTLED = source.DATESETTLED
                    ,BANKROUTINGNUMBER = source.BANKROUTINGNUMBER
                    ,CHECKNUMBER = source.CHECKNUMBER
                    ,AMOUNTDISBURSED = source.AMOUNTDISBURSED
                    ,STATUS = source.STATUS
                    ,NOTE = source.NOTE
                    ,DLN = source.DLN
                    ,SSN = source.SSN
                    ,ADDRESS1 = source.ADDRESS1
                    ,ADDRESS2 = source.ADDRESS2
                    ,CITY = source.CITY
                    ,STATE = source.STATE
                    ,ZIP = source.ZIP
                    ,CUSTOMEREMAIL = source.CUSTOMEREMAIL
                    ,BIRTHDATE = source.BIRTHDATE
                    ,DLNISSUESTATE = source.DLNISSUESTATE
                    ,NOCDISBURSED = source.NOCDISBURSED
                    ,NOCINFODATA = source.NOCINFODATA
                    ,NOCDATE = source.NOCDATE
                    ,ISLIVE = source.ISLIVE
                    ,DATELASTUPDATED = source.DATELASTUPDATED
                    ,BANKACCOUNTNOMASKED = source.BANKACCOUNTNOMASKED
                    ,ORIGINATEDATE = source.ORIGINATEDATE
                    ,REFUNDS = source.REFUNDS
                    ,REFUNDEDID = source.REFUNDEDID
                    ,SECCODE = source.SECCODE
                    ,DESCRIPTOR = source.DESCRIPTOR
                    ,WAREHOUSEID = source.WAREHOUSEID
                    ,BATCHUPLOADID = source.BATCHUPLOADID
                    ,AUTHID = source.AUTHID
                    ,MERCHANTNUMBER = source.MERCHANTNUMBER
                    ,CUSTREFID = source.CUSTREFID
                    ,START_DATE = source.START_DATE
                    ,END_DATE = source.END_DATE
                    ,CREATED_AT = convert_timezone('UTC', source.CREATED_AT)::timestamp_ntz
                when not matched then insert (
                    ID
                    ,OURREFERENCENUMBER
                    ,THEIRREFERENCENUMBER
                    ,APPROVALCODE
                    ,DATEPROCESSED
                    ,EFFECTIVEENTRYDATE
                    ,NAME
                    ,PHONENUMBER
                    ,INDIVIDUALID
                    ,AMOUNT
                    ,RETURNDATE
                    ,RETURNREASONCODE
                    ,RESUBMITTED
                    ,RESUBMITTEDREFERENCENUMBER
                    ,DATESETTLED
                    ,BANKROUTINGNUMBER
                    ,CHECKNUMBER
                    ,AMOUNTDISBURSED
                    ,STATUS
                    ,NOTE
                    ,DLN
                    ,SSN
                    ,ADDRESS1
                    ,ADDRESS2
                    ,CITY
                    ,STATE
                    ,ZIP
                    ,CUSTOMEREMAIL
                    ,BIRTHDATE
                    ,DLNISSUESTATE
                    ,NOCDISBURSED
                    ,NOCINFODATA
                    ,NOCDATE
                    ,ISLIVE
                    ,DATELASTUPDATED
                    ,BANKACCOUNTNOMASKED
                    ,ORIGINATEDATE
                    ,REFUNDS
                    ,REFUNDEDID
                    ,SECCODE
                    ,DESCRIPTOR
                    ,WAREHOUSEID
                    ,BATCHUPLOADID
                    ,AUTHID
                    ,MERCHANTNUMBER
                    ,CUSTREFID
                    ,START_DATE
                    ,END_DATE
                    ,CREATED_AT
                ) values (
                    source.ID
                    ,source.OURREFERENCENUMBER
                    ,source.THEIRREFERENCENUMBER
                    ,source.APPROVALCODE
                    ,source.DATEPROCESSED
                    ,source.EFFECTIVEENTRYDATE
                    ,source.NAME
                    ,source.PHONENUMBER
                    ,source.INDIVIDUALID
                    ,source.AMOUNT
                    ,source.RETURNDATE
                    ,source.RETURNREASONCODE
                    ,source.RESUBMITTED
                    ,source.RESUBMITTEDREFERENCENUMBER
                    ,source.DATESETTLED
                    ,source.BANKROUTINGNUMBER
                    ,source.CHECKNUMBER
                    ,source.AMOUNTDISBURSED
                    ,source.STATUS
                    ,source.NOTE
                    ,source.DLN
                    ,source.SSN
                    ,source.ADDRESS1
                    ,source.ADDRESS2
                    ,source.CITY
                    ,source.STATE
                    ,source.ZIP
                    ,source.CUSTOMEREMAIL
                    ,source.BIRTHDATE
                    ,source.DLNISSUESTATE
                    ,source.NOCDISBURSED
                    ,source.NOCINFODATA
                    ,source.NOCDATE
                    ,source.ISLIVE
                    ,source.DATELASTUPDATED
                    ,source.BANKACCOUNTNOMASKED
                    ,source.ORIGINATEDATE
                    ,source.REFUNDS
                    ,source.REFUNDEDID
                    ,source.SECCODE
                    ,source.DESCRIPTOR
                    ,source.WAREHOUSEID
                    ,source.BATCHUPLOADID
                    ,source.AUTHID
                    ,source.MERCHANTNUMBER
                    ,source.CUSTREFID
                    ,source.START_DATE
                    ,source.END_DATE
                    ,source.CREATED_AT
                )
            """
            )

            try:
                snowflake_connection.cursor().execute(merge_query)
            except ValueError:
                logger.error("Invalid value")
                raise AirflowFailException()
            except Exception as e:
                logger.error(f"Other error: {e}")
                raise AirflowFailException()
    else:
        logger.info(report_response.text)

@task
def get_and_load_effective_entry_date_transactions(MERCHANT_NUMBER: str, TOKEN: str, REPORT_URL: str, RAW_TABLE_NAME: str) -> None:
    """
    Fetches a dataframe of transactions  using effective entry date from the Check Commerce API and loads them to Snowflake
    :return: None
    """

    end_date = date.today()
    start_date = end_date - timedelta(days=1)

    start_date_string = start_date.strftime("%Y-%m-%d")
    end_date_string = end_date.strftime("%Y-%m-%d")

    report_params = {
        "Token": TOKEN,
        "Action": "Report",
        "OutputType": "JSON",
        "ReportPermDesc": "TransactionLog",
        "Parameter_CoNo": MERCHANT_NUMBER,
        "Parameter_EEDStart": start_date_string,
        "Parameter_EEDEnd": end_date_string
    }

    report_response = make_api_request(REPORT_URL, report_params)
        
    if report_response.status_code == 200:
        df = response_to_dataframe(report_response)

        if not df.empty:
            df['START_DATE'] = start_date_string
            df['END_DATE'] = end_date_string
            df['CREATED_AT'] = now("UTC")
            df.columns = df.columns.str.upper()
            suffix: str = "_UPDATES"
            dataframe_to_snowflake(
                df,
                database_name=RAW_DATABASE_NAME,
                schema_name=RAW_SCHEMA_NAME,
                table_name=f"{RAW_TABLE_NAME}{suffix}",
                overwrite=True,
                snowflake_connection=snowflake_connection,
            )
            merge_query: str = dedent(
                f"""
                merge into {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}
                    as target
                using {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}{suffix}
                    as source
                        on source.ID = target.ID
                            and source.START_DATE = target.START_DATE
                            and source.END_DATE = target.END_DATE                        
                when matched then update set
                    OURREFERENCENUMBER = source.OURREFERENCENUMBER
                    ,THEIRREFERENCENUMBER = source.THEIRREFERENCENUMBER
                    ,APPROVALCODE = source.APPROVALCODE
                    ,DATEPROCESSED = source.DATEPROCESSED
                    ,EFFECTIVEENTRYDATE = source.EFFECTIVEENTRYDATE
                    ,NAME = source.NAME
                    ,PHONENUMBER = source.PHONENUMBER
                    ,INDIVIDUALID = source.INDIVIDUALID
                    ,AMOUNT = source.AMOUNT
                    ,RETURNDATE = source.RETURNDATE
                    ,RETURNREASONCODE = source.RETURNREASONCODE
                    ,RESUBMITTED = source.RESUBMITTED
                    ,RESUBMITTEDREFERENCENUMBER = source.RESUBMITTEDREFERENCENUMBER
                    ,DATESETTLED = source.DATESETTLED
                    ,BANKROUTINGNUMBER = source.BANKROUTINGNUMBER
                    ,CHECKNUMBER = source.CHECKNUMBER
                    ,AMOUNTDISBURSED = source.AMOUNTDISBURSED
                    ,STATUS = source.STATUS
                    ,NOTE = source.NOTE
                    ,DLN = source.DLN
                    ,SSN = source.SSN
                    ,ADDRESS1 = source.ADDRESS1
                    ,ADDRESS2 = source.ADDRESS2
                    ,CITY = source.CITY
                    ,STATE = source.STATE
                    ,ZIP = source.ZIP
                    ,CUSTOMEREMAIL = source.CUSTOMEREMAIL
                    ,BIRTHDATE = source.BIRTHDATE
                    ,DLNISSUESTATE = source.DLNISSUESTATE
                    ,NOCDISBURSED = source.NOCDISBURSED
                    ,NOCINFODATA = source.NOCINFODATA
                    ,NOCDATE = source.NOCDATE
                    ,ISLIVE = source.ISLIVE
                    ,DATELASTUPDATED = source.DATELASTUPDATED
                    ,BANKACCOUNTNOMASKED = source.BANKACCOUNTNOMASKED
                    ,ORIGINATEDATE = source.ORIGINATEDATE
                    ,REFUNDS = source.REFUNDS
                    ,REFUNDEDID = source.REFUNDEDID
                    ,SECCODE = source.SECCODE
                    ,DESCRIPTOR = source.DESCRIPTOR
                    ,WAREHOUSEID = source.WAREHOUSEID
                    ,BATCHUPLOADID = source.BATCHUPLOADID
                    ,AUTHID = source.AUTHID
                    ,MERCHANTNUMBER = source.MERCHANTNUMBER
                    ,CUSTREFID = source.CUSTREFID
                    ,START_DATE = source.START_DATE
                    ,END_DATE = source.END_DATE
                    ,CREATED_AT = convert_timezone('UTC', source.CREATED_AT)::timestamp_ntz
                when not matched then insert (
                    ID
                    ,OURREFERENCENUMBER
                    ,THEIRREFERENCENUMBER
                    ,APPROVALCODE
                    ,DATEPROCESSED
                    ,EFFECTIVEENTRYDATE
                    ,NAME
                    ,PHONENUMBER
                    ,INDIVIDUALID
                    ,AMOUNT
                    ,RETURNDATE
                    ,RETURNREASONCODE
                    ,RESUBMITTED
                    ,RESUBMITTEDREFERENCENUMBER
                    ,DATESETTLED
                    ,BANKROUTINGNUMBER
                    ,CHECKNUMBER
                    ,AMOUNTDISBURSED
                    ,STATUS
                    ,NOTE
                    ,DLN
                    ,SSN
                    ,ADDRESS1
                    ,ADDRESS2
                    ,CITY
                    ,STATE
                    ,ZIP
                    ,CUSTOMEREMAIL
                    ,BIRTHDATE
                    ,DLNISSUESTATE
                    ,NOCDISBURSED
                    ,NOCINFODATA
                    ,NOCDATE
                    ,ISLIVE
                    ,DATELASTUPDATED
                    ,BANKACCOUNTNOMASKED
                    ,ORIGINATEDATE
                    ,REFUNDS
                    ,REFUNDEDID
                    ,SECCODE
                    ,DESCRIPTOR
                    ,WAREHOUSEID
                    ,BATCHUPLOADID
                    ,AUTHID
                    ,MERCHANTNUMBER
                    ,CUSTREFID
                    ,START_DATE
                    ,END_DATE
                    ,CREATED_AT
                ) values (
                    source.ID
                    ,source.OURREFERENCENUMBER
                    ,source.THEIRREFERENCENUMBER
                    ,source.APPROVALCODE
                    ,source.DATEPROCESSED
                    ,source.EFFECTIVEENTRYDATE
                    ,source.NAME
                    ,source.PHONENUMBER
                    ,source.INDIVIDUALID
                    ,source.AMOUNT
                    ,source.RETURNDATE
                    ,source.RETURNREASONCODE
                    ,source.RESUBMITTED
                    ,source.RESUBMITTEDREFERENCENUMBER
                    ,source.DATESETTLED
                    ,source.BANKROUTINGNUMBER
                    ,source.CHECKNUMBER
                    ,source.AMOUNTDISBURSED
                    ,source.STATUS
                    ,source.NOTE
                    ,source.DLN
                    ,source.SSN
                    ,source.ADDRESS1
                    ,source.ADDRESS2
                    ,source.CITY
                    ,source.STATE
                    ,source.ZIP
                    ,source.CUSTOMEREMAIL
                    ,source.BIRTHDATE
                    ,source.DLNISSUESTATE
                    ,source.NOCDISBURSED
                    ,source.NOCINFODATA
                    ,source.NOCDATE
                    ,source.ISLIVE
                    ,source.DATELASTUPDATED
                    ,source.BANKACCOUNTNOMASKED
                    ,source.ORIGINATEDATE
                    ,source.REFUNDS
                    ,source.REFUNDEDID
                    ,source.SECCODE
                    ,source.DESCRIPTOR
                    ,source.WAREHOUSEID
                    ,source.BATCHUPLOADID
                    ,source.AUTHID
                    ,source.MERCHANTNUMBER
                    ,source.CUSTREFID
                    ,source.START_DATE
                    ,source.END_DATE
                    ,source.CREATED_AT
                )
            """
            )

            try:
                snowflake_connection.cursor().execute(merge_query)
            except ValueError:
                logger.error("Invalid value")
                raise AirflowFailException()
            except Exception as e:
                logger.error(f"Other error: {e}")
                raise AirflowFailException()
    else:
        logger.info(report_response.text)

@task
def get_and_load_remittance_information(MERCHANT_NUMBER: str, TOKEN: str, REPORT_URL: str, RAW_TABLE_NAME: str) -> None:
    """
    Fetches a dataframe of remittance information from the Check Commerce API and loads them to Snowflake
    :return: None
    """

    end_date = date.today()
    start_date = end_date - timedelta(days=1)

    start_date_string = start_date.strftime("%Y-%m-%d")
    end_date_string = end_date.strftime("%Y-%m-%d")

    report_params = {
        "Token": TOKEN,
        "Action": "Report",
        "OutputType": "JSON",
        "ReportPermDesc": "RemittanceInformation",
        "Parameter_MID": MERCHANT_NUMBER,
        "Parameter_StartDate": start_date_string,
        "Parameter_EndDate": end_date_string    
    }

    report_response = make_api_request(REPORT_URL, report_params)
        
    if report_response.status_code == 200:
        df = response_to_dataframe(report_response)

        if not df.empty:
            df['START_DATE'] = start_date_string
            df['END_DATE'] = end_date_string
            df['CREATED_AT'] = now("UTC")
            df.columns = df.columns.str.upper()
            suffix: str = "_UPDATES"
            dataframe_to_snowflake(
                df,
                database_name=RAW_DATABASE_NAME,
                schema_name=RAW_SCHEMA_NAME,
                table_name=f"{RAW_TABLE_NAME}{suffix}",
                overwrite=True,
                snowflake_connection=snowflake_connection,
            )
            merge_query: str = dedent(
                f"""
                merge into {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}
                    as target
                using {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}{suffix}
                    as source
                        on source.REMITTANCEPK = target.REMITTANCEPK
                            and source.START_DATE = target.START_DATE
                            and source.END_DATE = target.END_DATE                        
                when matched then update set
                    WAREHOUSEID = source.WAREHOUSEID
                    ,WEBID = source.WEBID
                    ,EXTERNALID = source.EXTERNALID
                    ,EXTERNALSOURCE = source.EXTERNALSOURCE
                    ,COMPANY = source.COMPANY
                    ,COMPANYNAME = source.COMPANYNAME
                    ,SETTLEMENTED = source.SETTLEMENTED
                    ,CREDIT = source.CREDIT
                    ,DEBIT = source.DEBIT
                    ,COLLECTED = source.COLLECTED
                    ,DISBURSED = source.DISBURSED
                    ,EED = source.EED
                    ,ENTRYAMOUNT = source.ENTRYAMOUNT
                    ,ROUTINGNO = source.ROUTINGNO
                    ,ACCOUNTNO = source.ACCOUNTNO
                    ,IDENTNUM = source.IDENTNUM
                    ,ENTRYNAME = source.ENTRYNAME
                    ,ENTRYDESCRIPTION = source.ENTRYDESCRIPTION
                    ,TRACE15 = source.TRACE15
                    ,TRANS_CODE = source.TRANS_CODE
                    ,BATCHSTATUS = source.BATCHSTATUS
                    ,ORIGINATIONSTATUS = source.ORIGINATIONSTATUS
                    ,RETURNID = source.RETURNID
                    ,RETURNDATE = source.RETURNDATE
                    ,RETURNDESCRIPTION = source.RETURNDESCRIPTION
                    ,RETURNCODE = source.RETURNCODE
                    ,TAILEREDFIELDMID = source.TAILEREDFIELDMID
                    ,TAILEREDFIELDCGID = source.TAILEREDFIELDCGID
                    ,TAILEREDFIELDSTW = source.TAILEREDFIELDSTW
                    ,CHAINCODE = source.CHAINCODE
                    ,PARENTCHAINCODE = source.PARENTCHAINCODE
                    ,GLOBALMID = source.GLOBALMID
                    ,PARENTMID = source.PARENTMID
                    ,PARENTISPAYFAC = source.PARENTISPAYFAC
                    ,THEIRREFERENCENUMBER = source.THEIRREFERENCENUMBER
                    ,DATEMODIFIED = source.DATEMODIFIED
                    ,DISCOUNTFEE = source.DISCOUNTFEE
                    ,TRANSACTIONFEE = source.TRANSACTIONFEE
                    ,RETURNFEE = source.RETURNFEE
                    ,START_DATE = source.START_DATE
                    ,END_DATE = source.END_DATE
                    ,CREATED_AT = convert_timezone('UTC', source.CREATED_AT)::timestamp_ntz
                when not matched then insert (
                    WAREHOUSEID
                    ,REMITTANCEPK
                    ,WEBID
                    ,EXTERNALID
                    ,EXTERNALSOURCE
                    ,COMPANY
                    ,COMPANYNAME
                    ,SETTLEMENTED
                    ,CREDIT
                    ,DEBIT
                    ,COLLECTED
                    ,DISBURSED
                    ,EED
                    ,ENTRYAMOUNT
                    ,ROUTINGNO
                    ,ACCOUNTNO
                    ,IDENTNUM
                    ,ENTRYNAME
                    ,ENTRYDESCRIPTION
                    ,TRACE15
                    ,TRANS_CODE
                    ,BATCHSTATUS
                    ,ORIGINATIONSTATUS
                    ,RETURNID
                    ,RETURNDATE
                    ,RETURNDESCRIPTION
                    ,RETURNCODE
                    ,TAILEREDFIELDMID
                    ,TAILEREDFIELDCGID
                    ,TAILEREDFIELDSTW
                    ,CHAINCODE
                    ,PARENTCHAINCODE
                    ,GLOBALMID
                    ,PARENTMID
                    ,PARENTISPAYFAC
                    ,THEIRREFERENCENUMBER
                    ,DATEMODIFIED
                    ,DISCOUNTFEE
                    ,TRANSACTIONFEE
                    ,RETURNFEE
                    ,START_DATE
                    ,END_DATE
                    ,CREATED_AT
                ) values (
                    source.WAREHOUSEID
                    ,source.REMITTANCEPK
                    ,source.WEBID
                    ,source.EXTERNALID
                    ,source.EXTERNALSOURCE
                    ,source.COMPANY
                    ,source.COMPANYNAME
                    ,source.SETTLEMENTED
                    ,source.CREDIT
                    ,source.DEBIT
                    ,source.COLLECTED
                    ,source.DISBURSED
                    ,source.EED
                    ,source.ENTRYAMOUNT
                    ,source.ROUTINGNO
                    ,source.ACCOUNTNO
                    ,source.IDENTNUM
                    ,source.ENTRYNAME
                    ,source.ENTRYDESCRIPTION
                    ,source.TRACE15
                    ,source.TRANS_CODE
                    ,source.BATCHSTATUS
                    ,source.ORIGINATIONSTATUS
                    ,source.RETURNID
                    ,source.RETURNDATE
                    ,source.RETURNDESCRIPTION
                    ,source.RETURNCODE
                    ,source.TAILEREDFIELDMID
                    ,source.TAILEREDFIELDCGID
                    ,source.TAILEREDFIELDSTW
                    ,source.CHAINCODE
                    ,source.PARENTCHAINCODE
                    ,source.GLOBALMID
                    ,source.PARENTMID
                    ,source.PARENTISPAYFAC
                    ,source.THEIRREFERENCENUMBER
                    ,source.DATEMODIFIED
                    ,source.DISCOUNTFEE
                    ,source.TRANSACTIONFEE
                    ,source.RETURNFEE
                    ,source.START_DATE
                    ,source.END_DATE
                    ,source.CREATED_AT
                )
            """
            )

            try:
                snowflake_connection.cursor().execute(merge_query)
            except ValueError:
                logger.error("Invalid value")
                raise AirflowFailException()
            except Exception as e:
                logger.error(f"Other error: {e}")
                raise AirflowFailException()
    else:
        logger.info(report_response.text)

@task
def get_and_load_disbursement_information(MERCHANT_NUMBER: str, TOKEN: str, REPORT_URL: str, RAW_TABLE_NAME: str) -> None:
    """
    Fetches a dataframe of disbursment information from the Check Commerce API and loads them to Snowflake
    :return: None
    """

    end_date = date.today()
    start_date = end_date - timedelta(days=1)

    start_date_string = start_date.strftime("%Y-%m-%d")
    end_date_string = end_date.strftime("%Y-%m-%d")

    report_params = {
        "Token": TOKEN,
        "Action": "Report",
        "OutputType": "JSON",
        "ReportPermDesc": "DisbursementInformation",
        "Parameter_AggregatorMID": MERCHANT_NUMBER,
        "Parameter_StartDate": start_date_string,
        "Parameter_EndDate": end_date_string
    }

    report_response = make_api_request(REPORT_URL, report_params)
        
    if report_response.status_code == 200:
        df = response_to_dataframe(report_response)

        if not df.empty:
            df['START_DATE'] = start_date_string
            df['END_DATE'] = end_date_string
            df['CREATED_AT'] = now("UTC")
            df.columns = df.columns.str.upper()
            suffix: str = "_UPDATES"
            dataframe_to_snowflake(
                df,
                database_name=RAW_DATABASE_NAME,
                schema_name=RAW_SCHEMA_NAME,
                table_name=f"{RAW_TABLE_NAME}{suffix}",
                overwrite=True,
                snowflake_connection=snowflake_connection,
            )
            merge_query: str = dedent(
                f"""
                merge into {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}
                    as target
                using {RAW_DATABASE_NAME}.{RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}{suffix}
                    as source
                        on source.DISBURSEMENTPK = target.DISBURSEMENTPK
                            and source.START_DATE = target.START_DATE
                            and source.END_DATE = target.END_DATE                        
                when matched then update set
                    WAREHOUSEID = source.WAREHOUSEID
                    ,COMPANY = source.COMPANY
                    ,COMPANYNAME = source.COMPANYNAME
                    ,SETTLEMENTED = source.SETTLEMENTED
                    ,DISBURSEMENT = source.DISBURSEMENT
                    ,ORIGCOUNT = source.ORIGCOUNT
                    ,RETCOUNT = source.RETCOUNT
                    ,EED = source.EED
                    ,ENTRYAMOUNT = source.ENTRYAMOUNT
                    ,ROUTINGNO = source.ROUTINGNO
                    ,ACCOUNTNO = source.ACCOUNTNO
                    ,IDENTNUM = source.IDENTNUM
                    ,ENTRYNAME = source.ENTRYNAME
                    ,ENTRYDESCRIPTION = source.ENTRYDESCRIPTION
                    ,TRACE15 = source.TRACE15
                    ,TRANS_CODE = source.TRANS_CODE
                    ,BATCHSTATUS = source.BATCHSTATUS
                    ,ORIGINATIONSTATUS = source.ORIGINATIONSTATUS
                    ,ORIGINATIONDESCRIPTION = source.ORIGINATIONDESCRIPTION
                    ,RETURNID = source.RETURNID
                    ,RETURNDATE = source.RETURNDATE
                    ,RETURNDESCRIPTION = source.RETURNDESCRIPTION
                    ,RETURNCODE = source.RETURNCODE
                    ,TAILEREDFIELDMID = source.TAILEREDFIELDMID
                    ,TAILEREDFIELDCGID = source.TAILEREDFIELDCGID
                    ,TAILEREDFIELDSTW = source.TAILEREDFIELDSTW
                    ,CHAINCODE = source.CHAINCODE
                    ,PARENTCHAINCODE = source.PARENTCHAINCODE
                    ,GLOBALMID = source.GLOBALMID
                    ,PARENTMID = source.PARENTMID
                    ,PARENTISPAYFAC = source.PARENTISPAYFAC
                    ,THEIRREFERENCENUMBER = source.THEIRREFERENCENUMBER
                    ,DATEMODIFIED = source.DATEMODIFIED
                    ,START_DATE = source.START_DATE
                    ,END_DATE = source.END_DATE
                    ,CREATED_AT = convert_timezone('UTC', source.CREATED_AT)::timestamp_ntz
                when not matched then insert (
                    WAREHOUSEID
                    ,DISBURSEMENTPK
                    ,COMPANY
                    ,COMPANYNAME
                    ,SETTLEMENTED
                    ,DISBURSEMENT
                    ,ORIGCOUNT
                    ,RETCOUNT
                    ,EED
                    ,ENTRYAMOUNT
                    ,ROUTINGNO
                    ,ACCOUNTNO
                    ,IDENTNUM
                    ,ENTRYNAME
                    ,ENTRYDESCRIPTION
                    ,TRACE15
                    ,TRANS_CODE
                    ,BATCHSTATUS
                    ,ORIGINATIONSTATUS
                    ,ORIGINATIONDESCRIPTION
                    ,RETURNID
                    ,RETURNDATE
                    ,RETURNDESCRIPTION
                    ,RETURNCODE
                    ,TAILEREDFIELDMID
                    ,TAILEREDFIELDCGID
                    ,TAILEREDFIELDSTW
                    ,CHAINCODE
                    ,PARENTCHAINCODE
                    ,GLOBALMID
                    ,PARENTMID
                    ,PARENTISPAYFAC
                    ,THEIRREFERENCENUMBER
                    ,DATEMODIFIED
                    ,START_DATE
                    ,END_DATE
                    ,CREATED_AT
                ) values (
                    source.WAREHOUSEID
                    ,source.DISBURSEMENTPK
                    ,source.COMPANY
                    ,source.COMPANYNAME
                    ,source.SETTLEMENTED
                    ,source.DISBURSEMENT
                    ,source.ORIGCOUNT
                    ,source.RETCOUNT
                    ,source.EED
                    ,source.ENTRYAMOUNT
                    ,source.ROUTINGNO
                    ,source.ACCOUNTNO
                    ,source.IDENTNUM
                    ,source.ENTRYNAME
                    ,source.ENTRYDESCRIPTION
                    ,source.TRACE15
                    ,source.TRANS_CODE
                    ,source.BATCHSTATUS
                    ,source.ORIGINATIONSTATUS
                    ,source.ORIGINATIONDESCRIPTION
                    ,source.RETURNID
                    ,source.RETURNDATE
                    ,source.RETURNDESCRIPTION
                    ,source.RETURNCODE
                    ,source.TAILEREDFIELDMID
                    ,source.TAILEREDFIELDCGID
                    ,source.TAILEREDFIELDSTW
                    ,source.CHAINCODE
                    ,source.PARENTCHAINCODE
                    ,source.GLOBALMID
                    ,source.PARENTMID
                    ,source.PARENTISPAYFAC
                    ,source.THEIRREFERENCENUMBER
                    ,source.DATEMODIFIED
                    ,source.START_DATE
                    ,source.END_DATE
                    ,source.CREATED_AT
                )
            """
            )

            try:
                snowflake_connection.cursor().execute(merge_query)
            except ValueError:
                logger.error("Invalid value")
                raise AirflowFailException()
            except Exception as e:
                logger.error(f"Other error: {e}")
                raise AirflowFailException()
    else:
        logger.info(report_response.text)        

@dag(
    dag_id=this_filename,
    description="Extracts data from the Check Commerce API",
    tags=["data", "check_commerce", "payments"],
    schedule="0 13 * * *",  # Daily at 8am CT
    start_date=dag_start_date,
    max_active_runs=1,
    catchup=False,
    default_args=dict(
        owner="Data Engineering",
        start_date=dag_start_date,
        depends_on_past=False,
        retries=0,  # Manually retry only after manual dbt re-rerun
        retry_delay=duration(minutes=10),
        execution_timeout=duration(minutes=60),
        on_failure_callback=task_failure_slack_alert,
    ),
)
def checkcommerce_api_extract():
    get_and_load_merchant_list(MERCHANT_NUMBER, TOKEN, REPORT_URL, 'MERCHANT_LIST') >> get_and_load_merchant_information(MERCHANT_NUMBER, TOKEN, REPORT_URL, 'MERCHANT_INFORMATION')
    get_and_load_invoice_aggregate_line_items(MERCHANT_NUMBER, TOKEN, REPORT_URL, 'INVOICE_AGGREGATE_LINE_ITEMS')
    get_and_load_invoice_aggregate_demographic_data(MERCHANT_NUMBER, TOKEN, REPORT_URL, 'INVOICE_AGGREGATE_DEMOGRAPHIC_DATA')
    get_and_load_aggregator_merchant_invoice(MERCHANT_NUMBER, TOKEN, REPORT_URL, 'AGGREGATOR_MERCHANT_INVOICE')
    get_and_load_transaction_log(MERCHANT_NUMBER, TOKEN, REPORT_URL, 'TRANSACTION_LOG') >> get_and_load_returned_transactions(MERCHANT_NUMBER, TOKEN, REPORT_URL, 'TRANSACTION_LOG_RETURNS') >> get_and_load_effective_entry_date_transactions(MERCHANT_NUMBER, TOKEN, REPORT_URL, 'TRANSACTION_LOG_EFFECTIVE_ENTRY_DATE')
    get_and_load_remittance_information(MERCHANT_NUMBER, TOKEN, REPORT_URL, 'REMITTANCE_INFORMATION')
    get_and_load_disbursement_information(MERCHANT_NUMBER, TOKEN, REPORT_URL, 'DISBURSEMENT_INFORMATION')

checkcommerce_api_extract()
