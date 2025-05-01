# Spark Batch Processing

## Overview
The **Spark Batch Processing** directory contains scripts and configurations for processing large datasets in batch mode using Apache Spark. This directory is designed to facilitate efficient data processing workflows, leveraging Spark's capabilities to handle big data.

## Project Structure
- **Airflow_job.py**: This script defines an Apache Airflow job that orchestrates the execution of Spark batch processing tasks. It schedules and manages the workflow, ensuring that the Spark jobs run in the correct order and handle dependencies effectively.
- **Spark_batch_processing.py**: This script contains the main logic for processing data in batches using Apache Spark. It includes data ingestion, transformation, and output storage.

## Requirements
- Python 3.x
- Apache Spark
- Apache Airflow
- Required Python packages:
  - `pyspark`
  - `apache-airflow`



# Spark User Event Analysis (Spark Streaming)

## Overview
The **Spark User Event Analysis** project utilizes Apache Kafka and Apache Spark Streaming to mimic user clicks and process event data in real-time. This project logs user interactions as events in a Kafka topic, which are then consumed and transformed by Spark applications. The processed data is stored in Hive and combined into an enhanced order details table.

## Project Structure
- **Python_event_generator.py**: This script simulates user clicks and logs them as events in a Kafka topic.
- **Spark_order_details.py**: A Spark Streaming application that consumes order details from the Kafka topic, processes the data, and stores it in Hive.
- **Spark_user_click.py**: A Spark Streaming application that consumes user click events from the Kafka topic, processes them, and stores them in Hive.
- **Spark_combine_data.py**: This script combines the data from the Hive tables created by the previous Spark applications into a single table named `order_details_enhanced`.

## Requirements
- Python 3.x
- Apache Kafka
- Apache Spark
- Hive
- Required Python packages:
  - `confluent-kafka`
  - `pyspark`
  - `hive`

You can install the necessary Python packages using pip:

```bash
pip install confluent-kafka pyspark

```bash
pip install pyspark apache-airflow

