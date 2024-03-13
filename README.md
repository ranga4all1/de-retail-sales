# de-retail-sales
A complete data engineering project for Montgomery County of Maryland - Warehouse and Retail Sales.

In this comprehensive data engineering project, we’ll walk through the entire process, from extracting data from a CSV file to building a visualization-ready dataset.

![](images/de-retail-sales-1.jpg)

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

## Steps

1. Build GCP infra using terraform. If not, install terraform using this [link](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
    - Check that terraform is installed  
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
2. 