import boto3
import base64
from botocore.exceptions import ClientError
import json
import requests
import pandas as pd
import datetime
from datetime import timedelta
import pytz
import os
import loguru
from loguru import logger
import re
import io

# -------------------------------------
# Variables
# -------------------------------------

REGION = 'us-east-1'

start_time = datetime.datetime.now()
logger.info(f'Start time: {start_time}')

SHIPBOB_API_SECRET = os.environ['SHIPBOB_API_SECRET']

# -------------------------------------
# Functions
# -------------------------------------

# Check S3 Path for Existing Data
# -----------

def check_path_for_objects(bucket: str, s3_prefix:str):

  logger.info(f'Checking for existing data in {bucket}/{s3_prefix}')

  # Create s3 client
  s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

  # List objects in s3_prefix
  result = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix )

  # Instantiate objects_exist
  objects_exist=False

  # Set objects_exist to true if objects are in prefix
  if 'Contents' in result:
      objects_exist=True

      logger.info('Data already exists!')

  return objects_exist

# Delete Existing Data from S3 Path
# -----------

def delete_s3_prefix_data(bucket:str, s3_prefix:str):


  logger.info(f'Deleting existing data from {bucket}/{s3_prefix}')

  # Create an S3 client
  s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

  # Use list_objects_v2 to list all objects within the specified prefix
  objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)

  # Extract the list of object keys
  keys_to_delete = [obj['Key'] for obj in objects_to_delete.get('Contents', [])]

  # Check if there are objects to delete
  if keys_to_delete:
      # Delete the objects using 'delete_objects'
      response = s3_client.delete_objects(
          Bucket=bucket,
          Delete={'Objects': [{'Key': key} for key in keys_to_delete]}
      )
      logger.info(f"Deleted {len(keys_to_delete)} objects")
  else:
      logger.info("No objects to delete")


def get_current_inventory(api_secret:str):
    """
    GET current inventory from Shipbob

    Parameters
    ----------
    api_secret : str
        API secret (PAT token generated in Shipbob)

    Returns
    -------
    pd.DataFrame
        Dataframe containing all SKUs and their current inventory levels per ShipBob
    """


    url = 'https://api.shipbob.com/1.0/inventory'

    # Set up the request headers with the Bearer token
    headers = {
            "Authorization": f"Bearer {api_secret}"
        }

    # Send the GET request
    response = requests.get(url, headers=headers)

    response_json = json.loads(response.text)

    response_df = pd.json_normalize(response_json)

    return response_df

# --------------------------------------------------------------------------------------

# Extract current inventory from Shipbob API
inventory_df = get_current_inventory(api_secret=SHIPBOB_API_SECRET)


# Subset columns
current_inventory_df = inventory_df[['id','name','total_fulfillable_quantity', 'total_onhand_quantity',
       'total_committed_quantity', 'total_sellable_quantity',
       'total_awaiting_quantity', 'total_exception_quantity',
       'total_internal_transfer_quantity', 'total_backordered_quantity',
       'is_active']].copy()

# Get current data
current_date = pd.to_datetime('today').strftime('%Y-%m-%d')


# CONFIGURE BOTO  =======================================



AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_ACCESS_SECRET']


# Create s3 client
s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                          )

# Set bucket
BUCKET = os.environ['S3_PRYMAL']

# WRITE TO S3 =======================================

current_time = datetime.datetime.now()
logger.info(f'Current time: {current_time}')

# Log number of rows
logger.info(f'{len(current_inventory_df)} rows in current_inventory_df')
 

# Configure S3 Prefix
S3_PREFIX_PATH = f"shipbob/shipbob_inventory/partition_date={current_date}/shipbob_inventory_{current_date}.csv"

# Check if data already exists for this partition
data_already_exists = check_path_for_objects(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)

# If data already exists, delete it .. 
if data_already_exists == True:
   
   # Delete data 
   delete_s3_prefix_data(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)


logger.info(f'Writing to {S3_PREFIX_PATH}')


with io.StringIO() as csv_buffer:
    current_inventory_df.to_csv(csv_buffer, index=False)

    response = s3_client.put_object(
        Bucket=BUCKET, 
        Key=S3_PREFIX_PATH, 
        Body=csv_buffer.getvalue()
    )

    status = response['ResponseMetadata']['HTTPStatusCode']

    if status == 200:
        logger.info(f"Successful S3 put_object response for PUT ({S3_PREFIX_PATH}). Status - {status}")
    else:
        logger.error(f"Unsuccessful S3 put_object response for PUT ({S3_PREFIX_PATH}. Status - {status}")

