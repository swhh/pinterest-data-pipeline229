# pinterest-data-pipeline229

## Project Description

The purpose of this project is to create build a data pipeline with Python, Kafka, Databricks and AWS services that is similar to the pipeline used by Pinterest to process billions of data points using batch and streaming processing.

## File structure

- user_posting_emulation Python file to fetch data from AWS RDS and stream the records to three Kafka topics via a REST proxy built with Confluent and the AWS API Gateway
- requirements.txt detailing Python dependencies
- 0afff2eeb7e3.ipynb Databricks notebook, which processes Pinterest records with Pyspark on a Databricks clusters, cleaning the records and generating analytics insights from them
- 0afff2eeb7e3_dag Python file which runs the above notebook on a daily schedule via AWS MWAA