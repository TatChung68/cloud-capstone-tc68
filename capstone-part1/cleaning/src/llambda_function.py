import re, json, os, zipfile
import logging
import lcapstone_clean 

# prefix in bucket
SRC_PREFIX = 'RAW/'
DST_PREFIX = 'CLEAN/'

'''
# Logger setting
logger = logging.getLogger()
if 'DEBUG' in os.environ and os.environ['DEBUG'] == 'true':
    logger.setLevel(logging.DEBUG)
    logger.debug('Debug mode enabled!')
else:
    logger.setLevel(logging.INFO)
'''

file_path = '../sample_s3/1987/On_Time_Performance_1987_10.zip'

def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    # print(object_key[9:])
    # Eliminate src_prefix to match form of event
    key = re.sub(r'RAW/', '', object_key)
    csv_key = re.sub(r'\.zip$', '.csv', key)
    match = re.search(r'_(\d{4})_(\d{2})', object_key)

    year = match.group(1)
    month = match.group(2)
    # print(f"{year} - {month}")
    dst_path = os.path.join(f"{year}", f"{month}", csv_key)
    print(dst_path)
    #lcapstone_clean.handle_zipfile(bucket, SRC_PREFIX, DST_PREFIX, key)

if __name__ == "__main__":
    event_path = './event.json'
    # Open and read the JSON file
    with open(event_path, 'r') as file:
        event = json.load(file)  
    
    lambda_handler(event, context=None)

    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        print(zip_ref.namelist()[0])