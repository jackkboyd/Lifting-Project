# Weight Lifting Data Pipeline

## Overview
This repository contains a personal project that is  designed to enhance my skills / gain exposure in data engineering. My goal with this pipeline is to pull all of my weightlifting data, which is tracked with excel on my phone, into a visualization tool to gain insights on my training and track my progress. 

> [!IMPORTANT]
> After several months I am sunsetting this project so that I can focus more of my time on learning computer science fundamentals. I plan on refactoring this sometime in the future with higher quality programming! 
> Many architectual choices in this project are not optimal / are overkill. These choices are intentional and are here to gain exeprience with more tools + concepts.

## Infrastructure  

### Tools & Services  

| Service       | Badge                                                                                     |  
|---------------|-------------------------------------------------------------------------------------------|  
| AWS S3        | ![aws-s3](https://img.shields.io/badge/AWS_S3-569A31?style=flat-square&logo=amazons3&logoColor=white)   |  
| Apache Airflow| ![airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat-square&logo=apache-airflow&logoColor=white) |  
| Docker        | ![docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)      |  
| AWS Glue      | ![aws-glue](https://img.shields.io/badge/AWS_Glue-232F3E?style=flat-square&logo=amazons3&logoColor=white) |  
| dbt           | ![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white)              |  
| Looker        | ![looker](https://img.shields.io/badge/Looker-4285F4?style=flat-square&logo=looker&logoColor=white)      |  

### Databases  

| Database       | Badge                                                                                     |  
|----------------|-------------------------------------------------------------------------------------------|  
| PostgreSQL     | ![postgres](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white) |  
| Snowflake      | ![snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white)   |  

# Pipeline Overview

### 1. Excel to S3
- The first step in the pipeline is moving my local Excel file to an S3 bucket.
- This is completed with a combination of Task Scheduler, batch scripts, and Python scripts.
- The goal of this is to establish S3 as the true starting point for the project.

### 2. S3 to Postgres
- Python scripts pick up the latest Excel file on S3 and upload it to PostgreSQL.
- These scripts perform the following tasks:
  1. Normalize the data.
  2. Add **replace and append logic**, which deletes records in the target database based on replace keys found in the source data.
  3. Create new dimension members in the respective dimension tables and replace the codes in the fact table with newly generated IDs.

### 3. Postgres to S3
- Python scripts move data from PostgreSQL back to an S3 bucket in **Parquet file format**.
- While leveraging Parquet and another S3 bucket is unnecessary for this project, it provides experience with columnar data storage and Parquet handling.

### 4. S3 (Parquet Files) to Snowflake
- **AWS Glue** is used to migrate the Parquet files from S3 to Snowflake staging tables.

### 5. Snowflake Staging to Production
- Data from Snowflake staging tables are moved to production tables using **dbt (data build tool)**.
- This step includes minor data transformations and quality checks to ensure data integrity.

### 6. Google Looker
- **Google Looker** is leveraged to create a dashboard for the data.
- Looker uses views created in Snowflake to present the data in an interactive and visual format.

![Page 1](images/dataPipelineDiagram.png)

## Roadmap

### Current Architectural Issues
1. **Fix Relationship Between Routines and Workouts**
   - The relationship between routines and workouts needs to be corrected.
2. **Update Keys on Fact Lifts**
   - PKs on Fact Lifts table needs to be updated to account for multiple subbed out exercises

### Enhancements to Implement Later
1. **Update Security**
   - Ensure all SQL statements are paramaterized for enhanced security.
   
2. **Set Up OAuth for Snowflake**
   - Configure OAuth for both dbt and AWS Glue integration with Snowflake.

3. **Migrate Docker to EC2**
   - Move Airflow to an EC2 environment for better synergy with credentialing.

5. **Create a Movement IDs Table**
   - Add a table for movement IDs to significantly enhance functionality.
   - As of right now movements are strings on respective fact tables.

9. **Add Rejected Rows Table**
   - Create a table to log rejected rows for improved error tracking and troubleshooting.




