# Data Warehouse & Data Lake Project

**Course:** Data Warehouse and Data Lake Systems

**Group:** The Group

👥 **Team Members**

* Abishan Arumugavel
* Amir Shatrolli
* Sandra Deck

## 📖 Project Overview

This repository contains the source code and infrastructure logic for our semester project on Data Warehousing and Data Lakes using Amazon Web Services (AWS).

The pipeline creates an end-to-end solution that ingests data from multiple sources, manages raw storage, and performs data quality cleaning and transformation. The architecture utilizes a serverless approach to ensure scalability and cost-efficiency.

## Key Features

**Dynamic Ingestion:** Fetches data from two dynamic API sources and one static CSV source.

**Decoupled Architecture:** Utilizes a "Finder/Downloader" pattern via AWS SQS to handle large datasets efficiently.

**Data Lake Structure:** Organizes data into raw and cleaned layers within AWS S3.

**Flexible Execution:** Supports both Initial Load (historical backfill) and Daily Load (incremental updates) modes.

## 📂 Repository Structure

```
├── aws_lambda_functions/          # Source code for Data Import (Lambda)
├── aws_step_functions/            # Source code for AWS Lambda Schedules (Step Function)
├── AWS_Glue_functions_clean-data/ # Source code for Data Cleaning (Glue)
├── AWS_Athena_Views/              # SQL views for analytics and research questions
└── README.md
```

## ⚙️ Architecture & Workflow

**1. Data Import (Raw Data)**
The ingestion layer is built using AWS Lambda and orchestrated by AWS Step Functions.

**Pattern:** To handle API rate limits and execution timeouts, the process is split into two roles:

* **Finder:** Scans the source APIs/CSV to identify required records. It sends metadata messages to an SQS Queue.
* **Downloader:** Polls the SQS Queue, downloads the specific documents, and saves them to the S3 Bucket under the raw/ folder.
* **Triggers:** The Step Function can be triggered manually or via EventBridge (CloudWatch Events) for daily schedules.

**2. Data Cleaning**
Located in AWS_Glue_functions_clean-data.

**Process:** AWS Glue jobs pick up the raw JSON/CSV files from the raw/ S3 folder.

**Transformation:** The jobs perform data quality checks, clean the data (type casting, handling nulls), and store the result in the clean/ folder in S3.

---

## 📊 Analytics Layer (Amazon Athena)

After the data has been cleaned and stored in the S3 `clean/` layer, an analytics layer is implemented using **Amazon Athena**.

* External tables reference the cleaned Parquet files in S3.
* Analytical logic is encapsulated in **Athena SQL views** located in `AWS_Athena_Views`.
* The views prepare and aggregate the data required to answer the project’s research questions (RQ1–RQ4).

This approach separates data preparation from analytical logic and ensures reproducible analysis.

---

## 📈 Visualization Layer (Tableau)

The Athena views are used as the data source for dashboards created in **Tableau**.

For publishing and reproducibility reasons, the data is materialized as a **Tableau Extract (.hyper)**:

* No live connection to Athena is required for dashboard usage.
* Dashboards remain stable and independent of schema changes in the data lake.
* The extract represents the final analytical snapshot used for evaluation.

---

## 🚀 Deployment Guide (Step-by-Step)

Follow these steps to recreate the entire pipeline in your own AWS environment.

**Prerequisites**

* An active AWS Account.
* AWS CLI installed and configured.
* IAM permissions to create S3, Lambda, SQS, Glue, Step Functions, and Athena.

**Step 1: Set up Storage (S3)**

* Navigate to the S3 Console.
* Create a unique bucket (e.g., dw-dl-project-bucket-xyz).
* Inside the bucket, create two folders:

  * raw/
  * cleaned_data/

**Step 2: Set up Messaging (SQS)**

* Navigate to the Simple Queue Service (SQS) Console.
* Create a standard queue (e.g., data-ingestion-queue).
* Note: Copy the Queue URL; you will need this for the Lambda environment variables.

**Step 3: Deploy Lambda Functions**

* Locate code in: aws_lambda_functions

**A. Finder Function:**

1. Upload the code from the aws_lambda_functions folder and recreate the lambda functions.
2. Add an Environment Variable: SQS_QUEUE_URL: [Paste the URL from Step 2]
3. Add an Environment Variable: S3_BUCKET_NAME: [Name of bucket from Step 1]
4. MODE: initial or daily (default).
5. Permissions: Attach an IAM policy allowing sqs:SendMessage to your queue.
6. Permissions: Attach an IAM policy allowing s3:PutObject to your bucket.
7. Trigger: Add an SQS Trigger and select the queue created in Step 2.

**Step 4: Orchestration (Step Functions)**

1. Navigate to AWS Step Functions.
2. Create a State Machine.
3. Add a task to trigger the Finder Function created in Step 3.
4. (Optional) Add a Wait state or Success tracker to monitor the ingestion.

**Step 5: Set up Data Cleaning (AWS Glue)**

1. Locate code in: AWS_Glue_functions_clean-data
2. Navigate to the AWS Glue Console.
3. Create a generic Python Shell job.
4. Paste the code from the repository folder.
5. Permissions: Ensure the Glue Role has s3:GetObject (on raw) and s3:PutObject (on cleaned).
6. Configuration:

   * Source path: s3://[your-bucket]/raw/
   * Destination path: s3://[your-bucket]/cleaned_data/

## 🕹 Usage / Running the Pipeline

**Mode Selection:** The code is designed to handle two scenarios. You can set this via the payload sent to the Step Function or an Environment Variable in the Finder Lambda.

**Initial Load:**
Set mode to initial. The system will scrape all available historical data.

**Daily Load:**
Set mode to daily. The system will only fetch data for the current date/delta.

**Monitoring:**
Check CloudWatch Logs for Lambda execution details.
Check the SQS Console to ensure messages are being consumed (flight messages should go to 0).
Check S3 to see files populating in the raw and cleaned_data folders.
