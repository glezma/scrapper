import boto3
import os
import json

# AWS clients
s3 = boto3.client('s3')

# Environment variables
BUCKET_NAME = os.environ['BUCKET_NAME']

def handler(event, context):
    # Loop over each SNS message
    for record in event['Records']:
        # Get the message (URL set) from SNS
        url_set = json.loads(record['Sns']['Message'])

        # Process each URL (append 'processed')
        processed_urls = [url + '_processed' for url in url_set]

        # Save the processed URLs back to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f'processed_urls/processed_set_{record["Sns"]["Subject"].replace(" ", "_")}.txt',
            Body='\n'.join(processed_urls)
        )

    return {
        'statusCode': 200,
        'body': 'Processed URLs saved to S3'
    }
