import json
import csv
import boto3


def lambda_handler(event, context):
    region = 'eu-west-1'
    record_list = []
    try:
        s3 = boto3.client('s3')
        dynamodb = boto3.client('dynamodb', region_name=region)
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        print('Bucket: ', bucket, 'Key: ', key)
        csv_file = s3.get_object(Bucket=bucket, Key=key)
        record_list = csv_file["Body"].read().decode('utf-8').split('\n')
        csv_reader = csv.reader(record_list, delimiter=',', quotechar='"')
        for row in csv_reader:
            id = row[0]
            text = row[1]
            sentiment_score = row[2]
            sentiment = row[3]
            add_to_db = dynamodb.put_item(
                TableName='reddit_table',
                Item={
                    'id': {'S': str(id)},
                    'sentiment_score': {'S': str(sentiment_score)},
                    'sentiment': {'S': str(sentiment)},
                    'text': {'S': str(text)}
                })
            print("Successfully added")
    except Exception as e:
        print(str(e))

