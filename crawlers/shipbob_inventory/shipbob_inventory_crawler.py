import boto3
import botocore
from botocore.exceptions import ClientError
import loguru
from loguru import logger
import os

# ----------------------------------
# VARIABLES
# ----------------------------------

CRAWLERS_TO_RUN = ['shipbob_inventory']
REGION = 'us-east-1'

AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_ACCESS_SECRET']

# ----------------------------------
# FUNCTIONS
# ----------------------------------

# Function to run glue crawlers
def run_glue_crawler(crawler_name:str):

    logger.info(f'Running glue crawler: {crawler_name}')

    # Create an AWS Glue client
    glue_client = boto3.client('glue', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    try:
        # Trigger the Crawler run using the 'start_crawler' method
        response = glue_client.start_crawler(Name=crawler_name)

        logger.info(f"Crawler {crawler_name} started successfully.")

    except botocore.exceptions.ClientError as e:

        # log CrawlerRunningException error
        if e.response['Error']['Code'] == 'CrawlerRunningException':
            logger.info(f"Crawler {crawler_name} is already running.. unable to trigger a new run at this time")

            response = None

        # log other botocore errors
        else:
            logger.error(f"Botocore error: {e}")

    # Return response
    return response

# ----------------------------------
# EXECUTE CODE
# ----------------------------------

for crawler in CRAWLERS_TO_RUN: 
    run_glue_crawler(crawler_name=crawler)













