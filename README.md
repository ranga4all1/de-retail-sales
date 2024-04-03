# de-retail-sales

## Overview
A complete data engineering project for Montgomery County of Maryland - Warehouse and Retail Sales.

In this comprehensive data engineering project, we’ll walk through the entire process, from extracting data from a CSV file, ETL pipeline, workflow orchestration, and batch processsing to building a visualization-ready dataset in online analytical processing (`OLAP`) Data Warehouse using star-schema.

Overall, a data engineering project for retail sales data empowers business to extract actionable insights, drive innovation, and stay competitive in today's dynamic retail landscape. By harnessing the power of data, retailers can unlock new opportunities for growth, efficiency, and customer satisfaction. Streamlining data pipelines and automating data workflows significantly enhances operational efficiency within retail organization. By reducing manual effort, eliminating data silos, and ensuring data consistency and integrity, business can save time and resources while improving overall productivity.

The framework used in this project can be easily adapted to any other retail/Wholesale dataset.

## Dataset 
We will be using the Montgomery County of Maryland - Warehouse and Retail Sales dataset from data.gov

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

## Project structure

├── README.md           
├── batch              
├── extras             
├── images              
├── orchestration       
├── terraform-gcp       
└── terraform-mage      

    README.md           # Readme file
    batch               # Spark batch processing
    extras              # Extra resources that you can use for experimentation and/or adapting this framework to new projects
    images              # images used in README.md
    orchestration       # DAGs - 'mage' workflow orchestrator
    terraform-gcp       # Infra build in GCP - Data Lake(GCS Bucket) and Data warehouse(BigQuery dataset)
    terraform-mage      # Infra build in GCP - 'mage' workflow orchestrator

# Dev Setup

This project is entirely developed in the cloud using GitHub Codespaces that mimic the local system. A free version of GitHub Codespaces should suffice for this project.

GitHub Codespace: **de-retail-sales: 2 cores, 8 GB RAM, 32 GB storage** 


## Deployment Setup

This project uses Google Cloud (GCP) resources. A free version of GCP should suffice for this project. If needed, you can adapt this project to use other cloud providers by finding equivalent services.

Note: In the end, You may want to destroy resources used for this project to avoid recurring charges, if any.

## Steps

1. **Setup Google Cloud Platform (GCP)**

    - In Google Cloud console, create a new Google Cloud Platform (GCP) project. Give it a suitable name and note down PROJECT ID.

    - Select the project you created at the top drop down list of projects. Make sure to use this project going forward. 

    - Create a service account key to enable services used in this project to access your GCP account. When creating the key, use the following settings:

        - Go to `IAM & Admin` --> Service accounts
            - Click "Create Service Account".
            - Give it any name you like and click "Create and Continue".
            - In 'Grant this service account access to project', add below roles, then click "Continue".
                - Viewer + Storage Admin + Storage Object Admin + BigQuery Admin + Artifact registry reader + Artifact registry writer + Cloud run developer + Cloud SQL Admin + Service account token creator
            - click "Done".
        
            Note: In case of permissions issues, you can add "owner" role and test. Once it works, move back to granular access by adding only missing roles.
            
        - After you create your service account, download keys.
            - Select your service account from the list.
            - Select the "Keys" tab.
            - In the `ADD KEY` drop down menu, select "Create new key".
            - Leave the "Key Type" as JSON.
            - Click "Create" to create the key and download/rename/save the key file as `my-creds.json`.

2. **Clone this repo to your dev system(loacl or GitHub Codespace, VM in cloud etc.)**
    ```
    git clone https://github.com/ranga4all1/de-retail-sales.git
    cd de-retail-sales
    mkdir creds
    ```
    -  Copy `my-creds.json` to your dev system in `creds` folder. With GitHub codespaces, simple drap and drop gets this done.

    Note: Never upload/share credentials to internet/GitHub etc. This can be prevented by adding `creds` folder to `.gitignore`
    
3. **Build GCP infra using terraform.** 
    - Check that terraform is installed. If not, install terraform using this [link](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)  
    ```
    terraform --version
    ```
    - Update `variables.tf` file with your variables. Look for `default = ""` in each section and update it with your values.
    '''
    #### Sample values/format:
    PROJECT_ID: "<PROJECT_ID>"
    CREDENTIALS: "/workspaces/de-retail-sales/creds/my-creds.json"
    LOCATION: "US"
    REGION: "US-WEST2"
    BQ_DATASET_NAME: "de_retail_sales_dataset"
    GCS_BUCKET_NAME: "<PROJECT_ID>-bucket"
    GCS_STORAGE_CLASS: "standard"
    '''

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
4. **Build Mage workflow orchestrator**

    - Check that gcloud CLI is installed. If not, install using this [link](https://cloud.google.com/sdk/docs/install#linux)
    ```
    gcloud --version
    ```
    - Login using your service account credentials file. Replace KEY_FILE with 'path to your creds json file'
    ```
    gcloud auth login --cred-file=<KEY_FILE>
    ```
    - Verify. Replace PROJECT_ID with 'your GCP project id'
    ```
    gcloud auth list
    gcloud storage ls --project <PROJECT_ID>
    ```
    - update 'variables.tf file with your variables. Do not change mage dafaults.
    '''
    #### Sample values/format:
    PROJECT_ID: "<PROJECT_ID>"
    CREDENTIALS: "/workspaces/de-retail-sales/creds/my-creds.json"
    LOCATION: "US"
    REGION: "US-WEST2"
    ZONE: "US-WEST2-a"
    '''

    - Create mage infra. Answer `yes` when prompted.
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

5. **Mage workflow orchestration env configuration**

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

6. **Workflow orchestration: directed acyclic graph(DAG)**

    - Let's start by creating a pipeline and adding our 1st block, a python **data loader**
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

    - In Mage GUI, create schedule to run pipeline periodically
        - Click 'Triggers' and use below parameters
            Trigger type: Schedule
            Trigger name: monthly_ingest_retail_data_gcs
            Trigger description: Monthly data ingestion to run on 2nd day of each month
            Frequency: monthly
            Start date and time: Choose your start date/time
        - Click 'Save changes'
        - Click 'Enable trigger'

        ![mage-schedule](images/mage-schedule.png)
    
7. **Batch processing**

    **Option 1: Spark Dataproc Cluster (Apache spark/Managed apache Hadoop) on GCP**

    Note: Dataproc may need billing enabled on your project. In that case, skip to Option 2 to use local system.

    - Setting up a Spark Dataproc Cluster (Managed apache Hadoop)
        - In Google cloud console, search and click `Dataproc`
        - Click `enable` for Cloud Dataproc API (If not used/done previously)
        - Click 'Create Cluster' -> Cluster on compute engine' -> create with below parameters. Keep all others as default.
            name: `de-retail-sales-cluster`
            region: same as used for you gcs bucket
            zone: same as used for you gcs bucket
            cluster type: single node
            option components: Jupyter notbook and Docker
        - In Google cloud console, go to Google Cloud Storage(GCS) -> Note down temp bucket created by spark dataproc cluter in this format - `gs://[bucket]/.spark-bigquery-[jobid]-[UUID]`

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

    - PySpark on Local system (GitHub codespace)
        - Download latest Cloud Storage and Bigquery connector for Hadoop 3.x
        ```
        cd /home/codespace/bin
        wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
        wget https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.36.1.jar
        cd /workspaces/de-retail-sales/batch/
        ```
        - Run below commands in terminal
        ```
        cd batch/02-spark-local
        pip install pyspark
        ```
        - Update `batch-spark.py` with your parameters
        ```
        # Replace below with you parameters
            credentials_location = '/workspaces/de-retail-sales/creds/my-creds.json'
            project_id = 'woven-edge-412500'
            input_retail = 'gs://woven-edge-412500-de-retail-sales-bucket/retail_data/*'
            output = 'gs://woven-edge-412500-de-retail-sales-bucket/star-schema/'
        ```
        - Run `batch-spark.py`. This should process data and create star schema files in gcs bucket.
        ```
        python batch-spark.py
        ```
        - In Google cloud console, go to Cloud Storage and verify that star schema files are created in your gcs bucket. Delete files named `.SUCCESS` and keep all other parquet files.

    - In Google cloud console, go to BigQuery -> run queries from `load-data-bigquery.sql`. This should create star-schema in DWH and populate the data.

8. **Data Analysis - Dashboard - Looker studio**

    Note: Data Analysis/visualization is not focus of this project, though this step is performed to demonstrate that final data in BigQuery DWH (OLAP) is in right format to be consumed further by Data team.
    
    - Go to Looker Studio: `https://lookerstudio.google.com`
        - Click Blank report -> connect to data -> BigQuery -> <your-project-id> -> de-retail-sales -> select all 4 tables of the star schema.

        ![Data sources](images/data-sources.png)

        - Create a `Blended table` by joining dimension tables with fact table

        ![Blend data](images/blend-data.png)

        - Create your dashboard visualization.

        ![Dashboard](images/dashboard-de-retail-sales.png)

        - PDF version of dashboard is available in `images` folder.
        - Additionally, My dashboard is available at this [link](https://lookerstudio.google.com/s/pTpmX0LK1Ug). This may be teared down after 2 weeks. 

9. **Next steps**

    - In the next iteration of this project:

        - Fully automate end-to-end pipeline by creating/scheduling spark-submit job via Mage workflow orchestrator.
        - Data ingestion : Add backfill, parameterization etc.
        - make and CI/CD
