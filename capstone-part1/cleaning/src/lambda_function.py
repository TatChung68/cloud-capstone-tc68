import re, os
import logging
import capstone_clean 

# prefix in bucket
DST_PREFIX = 'CLEAN'

# Logger setting
logger = logging.getLogger()

if 'DEBUG' in os.environ and os.environ['DEBUG'] == 'true':
    logger.setLevel(logging.DEBUG)
    logger.debug('Debug mode enabled!')
else:
    logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    # Example: {"key": "RAW/1987/On_Time_Performance_1987_10.zip", ...}
    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']  
    capstone_clean.handle_zipfile(bucket, DST_PREFIX, object_key)