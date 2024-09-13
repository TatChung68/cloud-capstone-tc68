#!/usr/bin/python
import re
import json
import csv
import io
import os
import zipfile
import boto3
import logging

SELECTED_FIELDS = [
    'Year', 'Month', 'DayofMonth', 'DayOfWeek', 'FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
    'CRSDepTime', 'DepTime', 'DepDelay', 'DepDelayMinutes', 'CRSArrTime', 'ArrTime', 'ArrDelay', 'ArrDelayMinutes',
    'Cancelled'
]

s3client = boto3.client('s3')

logger = logging.getLogger()
if 'DEBUG' in os.environ and os.environ['DEBUG'] == 'true':
    logger.setLevel(logging.DEBUG)
    logger.debug('debug mode enabled.')
else:
    logger.setLevel(logging.INFO)


def handle_zipfile(bucket, dst_prefix, key):
    '''
        Extract and read a file and distribute .csv files
        according year and month
        And write to folder /year/month/

        bucket: 'teechoon68-capstone'
        dst_prefix: 'CLEAN' 
        key: 'RAW/1987/On_Time_Performance_1987_10.zip'
        
    '''
    try:
        # Download the zip file from S3 into a buffer
        zip_obj = s3client.get_object(Bucket=bucket, Key=key)
        zip_buffer = io.BytesIO(zip_obj['Body'].read())
        logging.info("here")
        # Open the zip file
        with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
            # Prepare buffers for each year-month combination
            buffers = {}

            # Iterate over each file in the zip archive
            for file_name in zip_ref.namelist():
                csv_data = zip_ref.read(file_name).decode('utf-8')
                csv_buffer = io.StringIO(csv_data)
                csv_reader = csv.reader(csv_buffer)
                
                # Skip the header row if present
                headers = next(csv_reader)
                
                # Process each row in the CSV
                for row in csv_reader:
                    # Extract year and month (assuming they are in specific columns)
                    year = int(row[0])  # Change index as needed
                    month = int(row[1]) # Change index as needed
                    date = int(row[2])

                    # Create a key for the year-month combination
                    key = f"{year}/{month}/{date}"
                    
                    # Initialize buffer for this year-month if it doesn't exist
                    if key not in buffers:
                        buffers[key] = io.StringIO()
                        csv_writer = csv.writer(buffers[key])
                        csv_writer.writerow(headers)  # Write header to each buffer
                    
                    # Write the row to the appropriate buffer
                    csv_writer = csv.writer(buffers[key])
                    csv_writer.writerow(row)

            # Upload each buffer to S3
            for key, buffer in buffers.items():
                buffer.seek(0)
                # Construct the S3 key
                date_file = "%4d_%02d_%02d.csv" % (year, month, date)
                dst_path = f"{dst_prefix}/{year}/{month}/{date_file}"
                # Upload the buffer to S3
                s3client.put_object(Bucket=bucket, Key=dst_path, Body=buffer.getvalue())
                logging.info(f"Uploaded {dst_path} to S3")
                
    except Exception as e:
        logging.error(f"Error processing and distributing CSV: {str(e)}")