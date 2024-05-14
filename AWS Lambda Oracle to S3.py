# Example code for what a AWS Lambda Oracle -> S3 Python Application may look like

import oracledb # Used for Oracledb connection install via: python -m pip install oracledb. Old driver name was cx_Oracle, maybe try that out if this causes errors.
import csv # To write data to CSV
import logging
import boto3 # AWS SDK
from botocore.exceptions import ClientError
import os

# Define function to upload CSV extracts to S3
def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    return True

def lambda_handler(event, context):
    # Set up Oracle connection details
    # This assumes these values have been saved as environment variables. They could also be imported from a different file or hard-coded.
    oracle_username = os.environ['ORACLE_USERNAME']
    oracle_password = os.environ['ORACLE_PASSWORD']
    oracle_host = os.environ['ORACLE_HOST']
    oracle_port = os.environ['ORACLE_PORT']
    oracle_service = os.environ['ORACLE_SERVICE']
    s3_bucket = os.environ['S3_BUCKET_NAME']

    # Connect to Oracle
    oracle_connection = oracledb.connect(
        user=oracle_username,
        password=oracle_password,
        dsn=f'{oracle_host}:{oracle_port}/{oracle_service}'
    )

    # Define list of Metasolv tables to ingest
    tables_to_ingest = ['ADDRESS', 'CIRCUIT', 'CIRCUIT_POSITION',
                        'CIRCUIT_POSITION_CONDITION', 'CIRCUIT_USER_DATA', 'CIRCUIT_XREF',
                        'CONDITION_CODE', 'CUST_ACCT', 'EQUIPMENT',
                        'EQUIPMENT_SPEC', 'EQUIPMENT_USER_DATA', 'MOUNTING_POSITION',
                        'MOUNTING_POSITION_CONDITION', 'NETWORK_LOCATION', 'NETWORK_NODE',
                        'NET_LOC_ADDR', 'NE_TYPE', 'PORT_ADDRESS',
                        'TRANSMISSION_RATE', 'USER_DATA_CATEGORY_VALUES']

    # Define empty list to store names of created CSV extracts
    csvs_to_upload = []

    # Iterate through tables to generate CSV extracts
    for table in tables_to_ingest:
        # Define your SQL query
        sql_query = f'SELECT * FROM database.schema.{table}'  # Assuming all tables are in the same db and schema. Replace with actual db and schema names.
        # Incremental load ideas
        # Maybe read the last modified date from the CSV files currently in S3?
        

        # Execute the query
        cursor = oracle_connection.cursor()
        cursor.execute(sql_query)

        # Fetch all rows from the result set
        rows = cursor.fetchall()

        # Define the path for the CSV file
        csv_file_path = f'/tmp/{table}_output.csv'  # Using /tmp directory for Lambda

        # Write the result to a CSV file
        with open(csv_file_path, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            
            # Write the column headers
            csv_writer.writerow([i[0] for i in cursor.description])
            
            # Write the data rows
            csv_writer.writerows(rows)

        csvs_to_upload.append(csv_file_path)  # add generated CSV files to list for reference in S3 uploads

        print(f"Data exported to {csv_file_path}")

    # Close cursor after fetching data for all tables
    cursor.close()

    # Close connection to Oracle DB after all files have been generated
    oracle_connection.close()

    # Upload the CSV files to S3
    for file in csvs_to_upload:
        s3_object_key = f's3_{os.path.basename(file)}'
        upload_file(file, s3_bucket, s3_object_key)
        print(f"CSV file uploaded to S3 bucket: {s3_bucket} with object key: {s3_object_key}")

    return {
        'statusCode': 200,
        'body': 'Data exported successfully and uploaded to S3'
    }
