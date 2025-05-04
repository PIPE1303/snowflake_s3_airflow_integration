# Monthly Account Balance Generation


## Overview

This project is an Apache Airflow pipeline for processing and generating monthly account balance files. The pipeline includes various tasks, such as creating a table in Snowflake, loading data from Snowflake to S3, and notifying a Slack channel about the availability of the generated CSV file. The project is designed to be run using [Astronomer](https://www.astronomer.io/), leveraging its environment and Airflow integration.


The project uses Airflow's **TaskFlow API** and integrates with Snowflake, AWS S3, and Slack.




# Account Monthly Balance Generation DAG

This project contains an Airflow DAG for generating and managing the monthly account balance data, which involves querying Snowflake, processing data, and uploading it to S3. Once the CSV file is uploaded to S3, a pre-signed URL is generated, and a Slack notification is sent.

## Project Structure

The project has the following folder structure:

```
/dags
    /account_monthly_balance_generation
        /queries
            create_table_snowflake.sql
            load_to_s3.sql
        account_monthly_balance_generation.py
/requirements.txt
/airflow_settings.yaml
/Dockerfile
```

### /dags

This folder contains the Python files for your Airflow DAGs. Currently, it includes the following:

- **account_monthly_balance_generation**: Contains the DAG for generating the monthly account balance.
  - **queries**: Folder containing SQL files required for Snowflake queries.
    - `create_table_snowflake.sql`: SQL file that creates the necessary tables in Snowflake.
    - `load_to_s3.sql`: SQL file to load data to an S3 bucket.
  - `account_monthly_balance_generation.py`: The Python DAG file that orchestrates the tasks.

### /include

A folder where you can place any additional files needed for your project (e.g., configuration files). This folder is empty by default.

### /plugins

A folder where you can place any global operators created for you (e.g., aws_operators.py). This folder is empty by default.

### /airflow_settings.yaml

This local-only file is used to specify Airflow connections, variables, and pools instead of manually entering them through the Airflow UI.

### /Dockerfile

This Dockerfile is used to configure and deploy the Airflow environment. It specifies the Airflow version and any additional configurations needed for the project.

### /packages.txt

Install OS-level packages needed for your project by adding them to this file. This file is empty by default.

### /requirements.txt

This file contains the Python packages required for the project. Ensure that the necessary libraries are installed, particularly the Airflow providers for Snowflake, Amazon, and Slack.

This project requires the following Python packages to function correctly:

- `apache-airflow-providers-amazon`
- `apache-airflow-providers-slack`
- `apache-airflow-providers-snowflake`
- `boto3`

- (Other dependencies can be added to `requirements.txt` as needed)


## How to Run

### Deploy Your Project Locally

To run the Airflow project locally using Astronomer, follow these steps:

1. Make sure you have Docker installed.
2. Run the following command to start the Airflow environment:
   ```bash
   astro dev start
   ```
3. Once the containers are ready, the Airflow UI will be available at `http://localhost:8080/`.
4. You can also access the Postgres Database at `localhost:5432/postgres` with the username `postgres` and password `postgres`.

### Deploy Your Project to Astronomer

If you have an Astronomer account, you can deploy the project to the Astronomer platform. Follow the instructions in the [Astronomer documentation](https://www.astronomer.io/docs/astro/deploy-code/) for deploying your project.

## Project Workflow

This DAG performs the following tasks:

1. **Create Table in Snowflake**: A SQL query is executed to create the necessary tables in Snowflake.
2. **Load Data to S3**: A SQL query loads the monthly account balance data to an S3 bucket.
3. **Generate Presigned URL**: A task generates a presigned URL for the uploaded CSV file in S3.
4. **Notify Slack**: Once the file is available, a Slack message is sent with the URL of the file.

## Requirements

- Python 3.8+
- Airflow 2.0+ (using Astronomer CLI)
- Snowflake connection configured in Airflow
- AWS S3 connection configured in Airflow
- Slack connection configured in Airflow

## Contact

For any questions or issues, feel free to reach out.

