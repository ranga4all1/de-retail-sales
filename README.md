# Overview: de-retail-sales
A complete data engineering project for Montgomery County of Maryland - Warehouse and Retail Sales.

In this comprehensive data engineering project, we’ll walk through the entire process, from extracting data from a CSV file, ETL pipeline, workflow orchestration, batch processsing to building a visualization-ready dataset in OLAP DWH using star-schema.

## Dataset 
We will be using Montgomery County of Maryland - Warehouse and Retail Sales dataset from data.gov

[County of Maryland - Warehouse and Retail Sales](https://catalog.data.gov/dataset/warehouse-and-retail-sales)

This dataset contains a list of sales and movement data by item and department appended monthly. 

- Dataset Information
    - Departments:	    Liquor Control, Department of
    - Update Frequency:	Monthly

- Topics
    - Category:	Community/Recreation
    - Tags:       liquor,alcohol,sale

- Licensing and Attribution
    - License:	Public Domain

- Columns in this Dataset

    | Column Name	      |   Description	                                      |   Type       |   
    |---------------------|-------------------------------------------------------|--------------|
    | YEAR                |   Calendar Year                                       |   Number     |
    | MONTH               |   Month                                               |   Number     |
    | SUPPLIER            |   Supplier Name                                       |   Plain Text |
    | ITEM CODE           |   Item code                                           |   Plain Text |
    | ITEM DESCRIPTION    |   Item Description                                    |   Plain Text |
    | ITEM TYPE	          |   Item Type                                           |   Plain Text |
    | RETAIL SALES	      |   Cases of product sold from DLC dispensaries         |   Number     |
    | RETAIL TRANSFERS    |   Cases of product transferred to DLC dispensaries    |   Number     |
    | WAREHOUSE SALES     |   Cases of product sold to MC licensees               |   Number     |

## Project Architecture

In this project, our initial focus was on establishing the essential data ingestion infrastructure within `Google Cloud Platform (GCP)`, leveraging `Terraform` to configure the necessary components, including Google Cloud Storage (GCS) buckets and BigQuery datasets. Subsequently, we utilized Terraform to deploy the `Mage workflow orchestrator` within the GCP environment.

Following the infrastructure setup, Mage facilitated the execution of our Extract-Transform-Load (`ETL`) pipeline, seamlessly orchestrating the extraction, transformation, and loading of data from web-based `CSV` sources into our designated GCS bucket, serving as a central data lake. Notably, the data was formatted into the efficient `Parquet` structure for optimized storage and processing.

For the transformation and analysis phase, we employed `Spark batch processing` capabilities, utilizing either Google Cloud `Dataproc` or local Spark instances for efficient computation. Through this process, we generated a comprehensive `star-schema`, enabling robust data modeling, and subsequently loaded the refined dataset into `BigQuery`, our Online Analytical Processing (`OLAP`) Data Warehouse.

The culmination of these efforts resulted in the provisioning of data in a structured format tailored to the requirements of our data team, facilitating seamless integration into their analytical and data science workflows.

To provide intuitive insights into the processed data, we leveraged `LookerStudio` to craft **dashboard** visualizations, effectively showcasing the usability and insights derived from our engineered data pipeline.

- High level architecture of the project:
![Project Architecture](images/de-retail-sales-1.jpg)

- Star schema:
![Star-schema](images/star-schema.png)

## Tech Stack

- Python
- Terraform - Infrastructure build
- Mage - workflow orchestrator
- Spark - Batch processing
- GCS bucket - Data lake
- BigQuery - OLAP Data Warehouse
- Looker Studio - Analytics dashboard

## Dev Setup

This project is entirely developed in cloud using GitHub Codespaces that mimics local system. A free version of GitHub Codespaces should suffice for this project.

## Deployment Setup

This project uses Google cloud (GCP) resources. A free version of GCP should suffice for this project.

Note: In the end, You may want to destroy resources used for this project to avoid recurring charges, if any.

## Steps

1. Clone this repo to your dev system(loacl or GitHub Codespace, VM in cloud etc.)
    ```
    git clone https://github.com/ranga4all1/de-retail-sales.git
    cd de-retail-sales
    ```

1. Build GCP infra using terraform. 
    - Check that terraform is installed. If not, install terraform using this [link](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)  
    ```
    terraform --version
    ```
    - Update `variables.tf` file with your variables
    - Update `.tf` files to terraform format 
    ```
    cd terraform-gcp
    terraform fmt
    ```

    - Download providers
    ```
    terraform init
    ```

    - See the plan
     ```
     terraform plan
     ```

    - apply the plan to create infra, answer `yes` when prompted.
    ```
    terraform apply
    ```
    - Verify in google cloud console that bucket and bigquerry dataset are created 
    - Destroy the infra (optional, at the end of the project to cleanup)
    ```
    terraform destroy
    ```
2. Build Mage workflow orchestrator

    - Check that gcloud CLI is installed. If not, install using this [link](https://cloud.google.com/sdk/docs/install#linux)
    ```
    gcloud --version
    ```
    - Login using your service account credentials file. Replace KEY_FILE with 'path to your creds json file'
    ```
    gcloud auth login --cred-file=KEY_FILE
    ```
    - Verify. Replace PROJECT_ID with 'your GCP project id'
    ```
    gcloud auth list
    gcloud storage ls --project PROJECT_ID
    ```
    - update 'variables.tf file with your variables and Create mage infra. Answer `yes` when prompted.
    ```
    cd terraform-mage/gcp
    terraform fmt
    terraform init
    terraform plan
    terraform apply
    ```
    - Destroy the infra (optional, at the end of the project to cleanup)
    ```
    terraform destroy
    ```
    - Verify in google cloud console that resources are created

    Notes:  
    1. Provide postgres password of your choice when prompted. 
    2. This step may fail initially after api enablement as it takes time for api to become available. In that case, rerun again.
    3. This step may take several minutes to complete.

3. Mage workflow orchestration env configuration

    - In Google cloud console, go to 'Cloud Run' -> Networking. Select 'All' in Ingress control and 'save'.
        Note:  For simplicity, we are allowing all access. You can go more granular by allowing your own ip address only.
    ![Mage Cloud Run](images/mage-cloud-run.png)

    - Click URL for your Cloud Run service. This should give you access to Mage workflow orchestrator GUI.
    ![Mage](images/mage-url.png)

    - In Mage workflow orchestrator GUI, click 'Files', Right click on 'default_repo' -> Upload files. Upload your mage service account credentials file. This is the credentials file that have access to required GCP resources sunch as gcs bucket and BigQuerry.
    - Click Terminal and run below commands from within `/home/src` dir to move that file to `/home/src/`
    ```
    bash
    pwd
    mv default_repo/<my-creds>.json my-creds.json
    ```
    - In Mage workflow orchestrator GUI, click 'Files' and locate `io_config.yaml` file and **delete or comment out** below section
    ```
    GOOGLE_SERVICE_ACC_KEY:
      type: service_account
      project_id: project-id
      private_key_id: key-id
      private_key: "-----BEGIN PRIVATE KEY-----\nyour_private_key\n-----END_PRIVATE_KEY"
      client_email: your_service_account_email
      auth_uri: "https://accounts.google.com/o/oauth2/auth"
      token_uri: "https://accounts.google.com/o/oauth2/token"
      auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs"
      client_x509_cert_url: "https://www.googleapis.com/robot/v1/metadata/x509/your_service_account_email"
    ```
    - Update this remaining section below it with `/home/src/my-creds.json`
    FROM:
    ```
    GOOGLE_SERVICE_ACC_KEY_FILEPATH: "/path/to/your/service/account/key.json"
    GOOGLE_LOCATION: US # Optional
    ```
    TO:
    ```   
    GOOGLE_SERVICE_ACC_KEY_FILEPATH: "/home/src/my-creds.json"
    GOOGLE_LOCATION: US # Optional
    ```

4. workflow orchestration

    - Let's start by creating a pipeline and adding our first block, a python **data loader**
        - Click Pipelines -> standard(batch)
        - Click Data Loader -> Python -> API
        - Use name: `load_retail_data`
        - Click 'Save and Add'
        - Replace or match template code with code from `orchestration/load_retail_data.py`
        - Save and Click 'Run block' button. Successful execution would show that test passed and also show first 10 rows of dataframe and shape of dataframe.
    - For easy reference later, Rename auto generated pipeline name to `ingest_retail_data_gcs`
    - Now Let's create a 2nd block - **Transformer**, directly below data loader block
        - Click Transformer -> Python -> Generic(no template)
        - Use name: `transform_retail_data`
        - Click 'Save and Add'
        - Replace or match template code with code from `orchestration/transform_retail_data.py`
        - Save and Click 'Run block' button. Successful execution would show that test passed and also show first 10 rows of dataframe and shape of dataframe.
    - Now Let's create a 3rd block - **Data Exporter**, directly below Transformer block
        - Click Data Exporter -> Python -> data lake -> Google CLoud Storage
        - Use name: `export_retail_data`
        - Click 'Save and Add'
        - Replace or match template code with code from `orchestration/export_retail_data.py`
        - Save and Click 'Run block' button. Successful execution would show green check mark.

        ![ETL pipeline](images/mage-etl-pipeline.png)

    - Go to GCP console and verify that data is exported into GCS bucket.

        ![GCS Bucket](images/gcs-bucket.png)
    
5. Batch processing

    **Option 1: Spark Dataproc Cluster (Managed apache Hadoop) on GCP**

    - Setting up a Spark Dataproc Cluster (Managed apache Hadoop)
        - In Google cloud console, search and click `Dataproc`
        - Click `enable` for Cloud Dataproc API (If not used/done previously)
        - Click 'Create Cluster' -> Cluster on compute engine' -> create with below parameters. Keep all others as default.
            name: `de-retail-sales-cluster`
            region: same as used for you gcs bucket
            zone: same as used for you gcs bucket
            cluster type: single node
            option components: Jupyter notbook and Docker
        - In Google cloud console, go to BigQuery -> Note down temp bucket created by spark dataproc cluter in this format - `gs://[bucket]/.spark-bigquery-[jobid]-[UUID]`

    - Update `batch/01-dataproc/batch-spark-bigquery.py` with below parameters and save.
        ```
        # Replace below with your GCP project_id
        project_id = '<your_project_id>'
        # Replace below with your temp bucket created by spark dataproc cluter
        spark.conf.set('temporaryGcsBucket', '<dataproc-temp-bucket>')
        ```
    - Upload `batch/01-dataproc/batch-spark-bigquery.py` to your GCS bucket as `code/batch-spark-bigquery.py`
    - in Terminal, submit a job to Dataproc cluster using BigQuery connector. Change parameters with your details.
        ```
        gcloud dataproc jobs submit pyspark \
            --cluster=de-retail-sales-cluster \
            --region=us-west1 \
            --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
            gs://woven-edge-412500-de-retail-sales-bucket/code/batch-spark-bigquery.py \
            -- \
                --input_retail=woven-edge-412500-de-retail-sales-bucket/retail_data/* \
                --output=woven-edge-412500.de_retail_sales_star_schema`
        ```
        Note: The connector writes the data to BigQuery by first buffering all the data into a Cloud Storage temporary table. Then it copies all data from into BigQuery in one operation. The connector attempts to delete the temporary files once the BigQuery load operation has succeeded and once again when the Spark application terminates. If the job fails, remove any remaining temporary Cloud Storage files. Typically, temporary BigQuery files are located in gs://[bucket]/.spark-bigquery-[jobid]-[UUID]
    - In Google cloud console, go to BigQuery and verify that dataset is created/populated with data as expected
    ![DWH](images/DWH.png) 

    **Option 2: PySpark on Local system (GitHub codespace)**

    - PySpark for testing on Local system (GitHub codespace)
        - Download latest Cloud Storage connector for Hadoop 3.x
        ```
        cd /home/codespace/bin
        wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
        wget https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.36.1.jar
        cd /workspaces/de-retail-sales/batch/
        ```
        - Run below commands in terminal
        ```
        cd batch
        pip install pyspark
        ```
        - Start jupyter-lab server for notebooks and connect to URL for jupyter
        ```
        jupyter-lab
        ```
        - Access and run notebook for spark - `batch-spark.ipynb`. This should process data and create star schema files in gcs bucket.
    
    - In Google cloud console, go to BigQuery -> run queries from `batch/load-data-bigquery`. This should create star-schema in DWH and populate the data.

6. Data Analysis - Dashboard - Looker studio

    Note: Data Analysis/visualization is not focus of this project, though this step is performed to demonstrate that final data in BigQuery DWH (OLAP) is in right format to be consumed further by Data team.
    
    - Go to Looker Studio: `https://lookerstudio.google.com`
        - Click Blank report -> connect to data -> BigQuery -> <your-project-id> -> de-retail-sales -> select all 4 tables of the star schema ending with np.

        ![Data sources](images/data-sources.png)

        - Create a `Blended table` by joining dimension tables with fact table

        ![Blend data](images/blend-data.png)

        - Create your dashboard visualization.

        ![Dashboard](images/dashboard-de-retail-sales.png)

        - PDF version of dashboard is available in `images` folder.
        - Additionally, My dashboard is available at this [link](https://lookerstudio.google.com/s/pTpmX0LK1Ug). This may be teared down after 2 weeks. 
