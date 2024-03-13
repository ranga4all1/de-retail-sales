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

