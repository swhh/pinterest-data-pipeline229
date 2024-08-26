# pinterest-data-pipeline229

## Project Description

The purpose of this project is to build a data pipeline with Python, Kafka, Databricks and AWS services - AWS API Gateway, AWS Kinesis, AWS RDS, AWS S3, AWS MSK and AWS MAA - similar to the pipeline used by Pinterest to process billions of data points using batch and stream processing.

The code streams data via APIs built with AWS API Gateway to Kafka and AWS Kinesis; the stored records are then fetched by Databricks notebooks where the data is cleaned and processed for analysis and storage purposes. 

<img width="843" alt="Screenshot 2024-08-26 at 18 27 46" src="https://github.com/user-attachments/assets/549e1b39-db4b-4c0c-9b70-28779388ec9a">

## File structure

- user_posting_emulation Python file to fetch data from AWS RDS and stream the records to three Kafka topics via a REST proxy built with Confluent and the AWS API Gateway
- requirements.txt detailing Python dependencies
- data_pipeline Databricks notebook, which processes Pinterest records with PySpark on a Databricks cluster, cleaning the records and generating analytics insights from them
- 0afff2eeb7e3_dag Python file which runs the above notebook on a daily schedule via AWS MWAA
- user_posting_emulation_streaming Python file to fetch data from AWS RDS and simulate a stream of data being sent to AWS Kinesis via an AWS Gateway API
- kinesis_pipeline is a  Databricks notebook, which fetches streaming data from AWS Kinesis, cleans the data and then stores it in delta tables. 

## Usage Instructions/Prerequisites

1. You will need to set up your own AWS and Databricks accounts.
2. You will need to set up an EC2 instance in AWS and set up Kafka on that instance.
3. You will need to create three streams within Kinesis and set up an AWS MSK cluster
4. You will need to set up a REST Proxy within your EC2 instance to send data to the AWS MSK cluster
5. You will need to set up APIs with the right endpoint structure (see image below), connect those endpoints to your REST proxy and Kinesis and make sure the API has the right permissions in AWS IAM
6. Run user_posting_emulation.py to send data to the AWS MSK cluster via the API Gateway and the REST proxy
7. Run the data_pipeline notebook within Databricks (after you have set up a compute cluster) to mount the S3 bucket hosting your Kafka data and then clean and process the fetched data
8. Run the Kinesis pipeline notebook in Databricks to stream data from AWS Kinesis, clean it and stream it to a Databricks delta table

### Streaming data from the pin Kinesis stream in the Databricks delta table
<img width="903" alt="Screenshot 2024-08-26 at 16 43 50" src="https://github.com/user-attachments/assets/0c037e06-11af-481c-9562-b8bffac5b36d">

### API Gateway API endpoint structure
<img width="1148" alt="Screenshot 2024-08-26 at 16 38 11" src="https://github.com/user-attachments/assets/32688de5-7ac7-4a48-b8f4-4085794df53d">



