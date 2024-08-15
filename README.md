# pinterest-data-pipeline229

## Project Description

The purpose of this project is to create build a data pipeline with Python, Kafka, Databricks and AWS services - AWS API Gateway, AWS Kinesis, AWS RDS, AWS S3, AWS AWS MKSS and AWS MAA - that is similar to the pipeline used by Pinterest to process billions of data points using batch and streaming processing.

The code streams data via APIs built with AWS API Gateway to Kafka and AWS Kinesis; the stored records are then fetched by Databricks notebooks where the data is cleaned and processed for analysis and storage purposes. 

## File structure

- user_posting_emulation Python file to fetch data from AWS RDS and stream the records to three Kafka topics via a REST proxy built with Confluent and the AWS API Gateway
- requirements.txt detailing Python dependencies
- 0afff2eeb7e3.ipynb Databricks notebook, which processes Pinterest records with Pyspark on a Databricks clusters, cleaning the records and generating analytics insights from them
- 0afff2eeb7e3_dag Python file which runs the above notebook on a daily schedule via AWS MWAA
- user_posting_emulation_streaming Python file to fetch data from AWS RDS and simulate a stream of data being sent to AWS Kinesis via an AWS Gateway API
- 0afff2eeb7e3_kinesis.ipynb Databricks notebook, which fetches streaming data from AWS Kinesis, cleans the data and then stores it in delta tables. 