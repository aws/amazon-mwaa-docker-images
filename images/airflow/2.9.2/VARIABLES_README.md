# Airflow Variables Setup

This document explains how to set up the required Airflow Variables for your DAGs.

## Quick Import (Recommended)

1. Open Airflow UI: http://localhost:8080
2. Go to: **Admin → Variables**
3. Click: **"Import Variables"** button (📤 icon)
4. Select: `airflow_variables.json`
5. Click: **Import**

Done! All 35 variables will be imported at once.

## Variables List

### 🔑 Core Variables (Update These First!)
| Variable | Default Value | Description |
|----------|---------------|-------------|
| `s3_bucket` | `your-s3-bucket-name` | Main S3 bucket for DAGs and data |
| `environment_name` | `dev` | Environment name (dev/staging/prod) |

### 📊 DBT Configuration
| Variable | Default Value | Description |
|----------|---------------|-------------|
| `dbt_model_selection` | `tag:default` | DBT model selection criteria |
| `source_schema` | `source_db` | Source database schema |
| `target_schema` | `target_db` | Target database schema |
| `include_schema_list` | `[]` | List of schemas to include |
| `exclude_schema_list` | `[]` | List of schemas to exclude |

### 📅 Date Configuration
| Variable | Default Value | Description |
|----------|---------------|-------------|
| `start_date` | `2025-10-22` | Default start date for processing |
| `end_date` | `2025-10-23` | Default end date for processing |
| `date_range_days` | `1` | Number of days in date range |

### ⚙️ Run Configuration
| Variable | Default Value | Description |
|----------|---------------|-------------|
| `is_full_load_run` | `N` | Flag for full load runs |
| `is_first_time_run` | `N` | Flag for first-time runs |
| `batch_id` | `0` | Batch identifier |
| `submit_job` | `true` | Whether to submit jobs |
| `dag_name` | `default_dag` | DAG name for operations |
| `core_name` | `default_core` | Core name identifier |

### ☁️ AWS Batch Configuration
| Variable | Default Value | Description |
|----------|---------------|-------------|
| `Aws_batch_definition` | `your-batch-job-definition` | Default AWS Batch job definition |
| `Aws_batch_definition_small` | `your-batch-job-definition-small` | Small job definition |
| `Aws_batch_definition_large` | `your-batch-job-definition-large` | Large job definition |
| `aws_batch_job_queue` | `your-batch-job-queue` | AWS Batch job queue |

### 🗄️ S3 Bucket Configuration
| Variable | Default Value | Description |
|----------|---------------|-------------|
| `rdf_files_s3` | `your-rdf-files-bucket` | S3 bucket for RDF files |
| `rdf_client` | `default-client` | RDF client identifier |
| `dw_oracle_bucket` | `your-dw-oracle-bucket` | Data warehouse Oracle bucket |
| `source_data_bucket` | `your-source-data-bucket` | Source data bucket |

### 🔄 DMS Configuration
| Variable | Default Value | Description |
|----------|---------------|-------------|
| `task_automation_dms_role_arn` | `arn:aws:iam::123456789012:role/your-dms-role` | DMS IAM role ARN |

### 📊 EMR Serverless Configuration
| Variable | Default Value | Description |
|----------|---------------|-------------|
| `emr_serverless_application_id` | `your-emr-app-id` | EMR Serverless application ID |
| `emr_serverless_job_role` | `arn:aws:iam::123456789012:role/your-emr-role` | EMR job IAM role ARN |
| `emr_serverless_bucket` | `your-emr-bucket` | EMR S3 bucket |
| `emr_serverless_bucket_folder` | `emr-scripts` | EMR scripts folder |
| `emr_serverless_data_bucket` | `your-emr-data-bucket` | EMR data bucket |
| `emr_serverless_script_path` | `s3://your-emr-bucket/scripts/` | EMR script path |

### 🔧 Other Configuration
| Variable | Default Value | Description |
|----------|---------------|-------------|
| `ad_hoc_rdf_files_conf` | `{}` | Ad-hoc RDF files configuration (JSON) |
| `genesys_ivr_secret_name` | `analytics-and-insights-airflow/genesys_ivr` | Secrets Manager secret name |
| `snow_oracle_job_def` | `your-snow-oracle-job-def` | Snow Oracle job definition |
| `snow_oracle_job_queue` | `your-snow-oracle-job-queue` | Snow Oracle job queue |

## After Import

1. **Verify Import**: Go to Admin → Variables and confirm all 35 variables are present
2. **Update Placeholder Values**: Replace `your-*` placeholders with actual values
3. **Restart Scheduler** (if needed): `docker restart mwaa-292-scheduler`
4. **Check DAGs**: Your DAGs should now load without errors

## Troubleshooting

### DAGs still showing errors?
- Verify all variables are set correctly
- Check for typos in variable names
- Restart the scheduler: `docker restart mwaa-292-scheduler`

### Can't import JSON file?
- Try adding variables manually through UI
- Use the **+** button in Admin → Variables
- Copy values from `airflow_variables.json`

## Files Created

- `airflow_variables.json` - JSON file for bulk import
- `setup_variables.py` - Python script (alternative method)
- `setup_variables.sh` - Shell script (alternative method)
- `VARIABLES_README.md` - This documentation

