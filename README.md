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

This project is entirely developed in cloud using GitHub Codespaces. A free version of GitHub Codespaces should suffice for this project.

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

    