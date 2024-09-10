import boto3
import csv
import os

# AWS clients
s3 = boto3.client('s3')
sns = boto3.client('sns')

# Environment variables
BUCKET_NAME = os.environ['BUCKET_NAME']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
N = 100  # Number of URLs in each set

def handler(event, context):
    # Get the CSV file from S3
    csv_file_key = 'url_list.csv'  # CSV file path in the bucket
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=csv_file_key)
    csv_data = obj['Body'].read().decode('utf-8').splitlines()

    # Read CSV data
    urls = list(csv.reader(csv_data))

    # Split the URL list into smaller sets
    url_sets = [urls[i:i + N] for i in range(0, len(urls), N)]

    # Publish each set of URLs to the SNS topic
    for index, url_set in enumerate(url_sets):
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=str(url_set),  # Send URLs as string
            Subject=f'URL Set {index + 1}'
        )

    return {
        'statusCode': 200,
        'body': f'Successfully split {len(urls)} URLs into {len(url_sets)} sets and published to SNS'
    }
