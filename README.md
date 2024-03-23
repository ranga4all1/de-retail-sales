# de-retail-sales
A complete data engineering project for Montgomery County of Maryland - Warehouse and Retail Sales.

In this comprehensive data engineering project, we’ll walk through the entire process, from extracting data from a CSV file to building a visualization-ready dataset.

![Project Architecture](images/de-retail-sales-1.jpg)

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

## Dev Setup

This project is entirely developed in cloud using GitHub Codespaces that mimics local system. A free version of GitHub Codespaces should suffice for this project.

## Deployment setup

This project uses Google cloud (GCP) resources. A free version of GCP should suffice for this project.

Note: In the end, You may want to destroy resources used for this project to avoid recurring charges, if any.

## Steps

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

    - apply the plan to create infra
    ```
    terraform apply
    yes
    ```
    - Verify in google cloud console that bucket and bigquerry dataset are created 
    - Destroy the infra (optional, at the end of the project to cleanup)
    ```
    terraform destroy
    yes
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
    - Download mage terraform templates
    ```
    mkdir terraform-mage
    cd terraform-mage/
    git clone https://github.com/mage-ai/mage-ai-terraform-templates.git
    cp -Rn mage-ai-terraform-templates .
    cd gcp
    ```
    - update 'variables.tf file with your variables. Additionally add this code block to that file.
    ```
        variable "credentials" {
        description = "my credentials"
        default     = "CREDENTIALS"
        # Replace CREDENTIALS with 'path to your creds json file'
        # Do NOT upload credentials files to github to repository or anywhere on internet
        # This can be done by adding those files/folders to .gitignore
        }
    ```
    - Check and update 'main.tf' file with below code 
        ```
        provider "google" {
          credentials = file(var.credentials)
          project     = var.project_id
          region      = var.region
          zone        = var.zone
        }

        provider "google-beta" {
          credentials = file(var.credentials)
          project     = var.project_id
          region      = var.region
          zone        = var.zone
        }

        # Enable filestore API
        resource "google_project_service" "filestore" {
          service            = "file.googleapis.com"
          disable_on_destroy = false
        }
        ```
    - Create mage infra
    ```
    terraform fmt
    terraform init
    terraform plan
    terraform apply
    yes
    ```
    - Destroy the infra (optional, at the end of the project to cleanup)
    ```
    terraform destroy
    yes
    ```
    - Verify in google cloud console that resources are created

    Notes:  
    1. Provide postgres password of your choice when prompted. 
    2. This step may fail initially after api enablement. Rerun again.
    3. This step may take several minutes to complete.

3. Mage workflow orchestration env configuration

    - In Google cloud console, go to 'Cloud Run' -> Networking. Select 'All' in Ingress control and 'save'.
        Note:  For simplicity, we are allowing all access. You can go more granular by allowing your own ip address only.
    - Click URL for your Cloud Run service. This should give you access to Mage workflow orchestrator GUI.
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
    
    - Go to Looker Studio: `https://lookerstudio.google.com`
        - Click Blank report -> connect to data -> BigQuery -> <your-project-id> -> de-retail-sales -> select all 4 tables of the star schema ending with np.

        ![Data sources](images/data-sources.png)

        - Create a `Blended table` by joining dimension tables with fact table

        ![Blend data](images/blend-data.png)

        - Create your dashboard visualization.

        ![Dashboard](images/dashboard-de-retail-sales.png)

        - Additionally, My dashboard is available at this [link](https://lookerstudio.google.com/s/pTpmX0LK1Ug). Also, pdf version of dashboard is available in `images` folder.


------------------------------------------------------

TO-DO:

    - Setting up a Spark Dataproc Cluster (Managed apache Hadoop)
        - In Google cloud console, search and click `Dataproc`
        - Click `enable` for Cloud Dataproc API (If not used/done previously)
        - Click 'Create Cluster' -> Cluster on compute engine' -> create with below parameters. Keep all others as default.
            name: `de-retails-sales-cluster`
            region: same as used for you gcs bucket
            zone: same as used for you gcs bucket
            cluster type: single node
            option components: Jupyter notbook and Docker
        -  