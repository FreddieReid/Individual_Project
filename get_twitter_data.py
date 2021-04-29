import csv
from airflow import DAG
import json
import pandas as pd
import numpy as np
import io
import requests
import os
import re
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks import aws_lambda_hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow import providers
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook

from datetime import datetime
from datetime import timedelta
import logging




default_args = {
    'start_date': datetime(2021, 3, 30),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'bucket_name': 'individualtwitter',
    'prefix': 'test_folder',
    'aws_conn_id': "aws_default_FreddieReid",
    'bearer_token': Variable.get("bearer_token", deserialize_json=True)['bearer_token'],
    'postgres_conn_id': 'postgres_conn_id_FreddieReid',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'output_key': Variable.get("twitter_output", deserialize_json=True)['output_key'],
    'db_name': Variable.get("schema_postgres_FreddieReid", deserialize_json=True)['db_name']
}

dag = DAG('twitter_individual',
      description = 'get twitter data',
      schedule_interval = '@daily',
      catchup = False,
      default_args = default_args,
      max_active_runs = 1)

log = logging.getLogger(__name__)


#def create_postgres_tables(**kwargs):
#    pg_hook = PostgresHook(postgres_conn_id=kwargs["postgres_conn_id"], schema=kwargs['db_name'])
#    conn = pg_hook.get_conn()
#    cursor = conn.cursor()

#    sql_queries = """
#        CREATE SCHEMA IF NOT EXISTS bitcoin_info;
#        DROP TABLE IF EXISTS bitcoin_info.recent_tweets;
#        CREATE TABLE bitcoin_info.recent_tweets(
#            id numeric,
#            text varchar,
#            sentiment_score numeric,
#            sentiment numeric
#        );
#
#        DROP TABLE IF EXISTS bitcoin_info.recent_reddit;
#        CREATE TABLE bitcoin_info.recent_reddit(
#            id numeric,
#            text varchar,
#            sentiment_score numeric,
#            sentiment numeric
#        );
#        """

#    cursor.execute(sql.queries)
#    conn.commit()
#    log.info("created schema and tables")


def collect_data(**kwargs):

    # get log information
    log.info('received:{0}'.format(kwargs))
    log.info('default arguments received:{0}'.format(kwargs))

    task_instance = kwargs['ti']

    # collect Twitter data from API
    bearer_token = kwargs["bearer_token"]

    # parameters for API search
    query = "bitcoin"
    max_results = 100

    # Prepare the headers to pass the authentication to Twitter's api
    headers = {
        'Authorization': 'Bearer {}'.format(bearer_token),
    }

    params = (
        ('query', query),
        ('max_results', str(int(max_results))),  # Let's make sure that the number is an string
    )

    # Does the request to get the most recent tweets
    response = requests.get('https://api.twitter.com/2/tweets/search/recent', headers=headers, params=params)

    # Let's convert the query result to a dictionary that we can iterate over
    tweets = json.loads(response.text)
    tweets_data = tweets["data"]

    return tweets_data


# store data in S3 bucket

def upload_to_s3(**kwargs):

    bucket_name = kwargs['bucket_name']
    key = kwargs['output_key']
    s3 = S3Hook(kwargs['aws_conn_id'])

    # Get the task instance
    task_instance = kwargs['ti']

    # Get the output of the previous task
    collected_data = task_instance.xcom_pull(
        task_ids="collect_data")

    log.info('xcom from collecting data task:{0}'.format(
        collected_data))

    # create dataframe for collected data
    tweets_df = pd.DataFrame(collected_data)

    # Prepare the file to send to s3
    csv_buffer = io.StringIO()
    tweets_df.to_csv(csv_buffer, index=False)

    # Save the pandas dataframe as a csv to s3
    s3 = s3.get_resource_type('s3')

    # Get the data type object from pandas dataframe, key and connection object to s3 bucket
    data = csv_buffer.getvalue()

    print("Saving CSV file")
    object = s3.Object(bucket_name, key)

    # Write the file to S3 bucket in specific path defined in key
    object.put(Body=data)

    log.info('Finished saving the scraped data to s3')


def lambda_preprocessing(**kwargs):
    hook = AwsLambdaHook('twitterpreprocessing',
                         region_name='eu-west-1',
                         log_type='None', qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         config=None, aws_conn_id='aws_default_FreddieReid')
    response_1 = hook.invoke_lambda(payload='null')
    log.info("preprocessed the twitter data")
    print('Response--->', response_1)


def create_sentiment_labels(**kwargs):
    hook = AwsLambdaHook('textblob',
                         region_name='eu-west-1',
                         log_type='None', qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         config=None, aws_conn_id='aws_default_FreddieReid')
    response_1 = hook.invoke_lambda(payload='null')
    log.info("added sentiment labels to twitter data")
    print('Response--->', response_1)


def collect_process_label_reddit_data(**kwargs):
    hook = AwsLambdaHook('redditbitcoin',
                         region_name='eu-west-1',
                         log_type='None', qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         config=None, aws_conn_id='aws_default_FreddieReid')
    response_1 = hook.invoke_lambda(payload='null')
    log.info("collected r/bitcoin data and added sentiment labels")
    print('Response--->', response_1)

def twitter_to_dynamo_db(**kwargs):
    hook = AwsLambdaHook('twitter_to_dynamo',
                         region_name='eu-west-1',
                         log_type='None', qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         config=None, aws_conn_id='aws_default_FreddieReid')
    response_1 = hook.invoke_lambda(payload='null')
    log.info("sent twitter data to dynamo db")
    print('Response--->', response_1)

def reddit_to_dynamo_db(**kwargs):
    hook = AwsLambdaHook('reddit_to_dynamo',
                         region_name='eu-west-1',
                         log_type='None', qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         config=None, aws_conn_id='aws_default_FreddieReid')
    response_1 = hook.invoke_lambda(payload='null')
    log.info("sent twitter data to dynamo db")
    print('Response--->', response_1)

# load twitter data from s3

def retrieve_twitter_data(**kwargs):
    key_twitter = Variable.get("twitter_labelled", deserialize_json=True)["key"]

    s3 = S3Hook(kwargs['aws_conn_id'])

    bucket_name = kwargs["bucket_name"]

    bytes_object = s3.read_key(key_twitter, bucket_name)

    twitter_data = pd.read_csv(io.StringIO(bytes_object), index_col = 0)

    twitter_data = twitter_data.to_dict()

    return twitter_data

def retrieve_reddit_data(**kwargs):
    key_reddit = Variable.get("reddit_labelled", deserialize_json=True)["key"]

    s3 = S3Hook(kwargs['aws_conn_id'])

    bucket_name = kwargs["bucket_name"]

    bytes_object1 = s3.read_key(key_reddit, bucket_name)

    reddit_data = pd.read_csv(io.StringIO(bytes_object1), index_col = 0)

    reddit_data = reddit_data.to_dict()

    return reddit_data

def create_summary_document(**kwargs):
    task_instance = kwargs['ti']
    s3 = S3Hook(kwargs['aws_conn_id'])

    collected_twitter_data = task_instance.xcom_pull(
        task_ids="retrieve_twitter_data")

    collected_reddit_data = task_instance.xcom_pull(
        task_ids="retrieve_reddit_data")

    twitter_df = pd.DataFrame(collected_twitter_data)
    reddit_df = pd.DataFrame(collected_reddit_data)

    average_twitter_sentiment = twitter_df["sentiment_score"].mean()

    average_reddit_sentiment = reddit_df["sentiment_score"].mean()

    a = ("Today, the average sentiment on bitcoin on twitter is: ")

    b = ("Today, the average sentiment on bitcoin on reddit is: ")


    # create dataframe with average results:

    frame = pd.DataFrame({'text': [a, b], 'avg_sentiment': [average_twitter_sentiment, average_reddit_sentiment]})

    # Prepare the file to send to s3
    csv_buffer = io.StringIO()
    frame.to_csv(csv_buffer, index=False)

    # Save the pandas dataframe as a csv to s3
    s3 = s3.get_resource_type('s3')

    # Get the data type object from pandas dataframe, key and connection object to s3 bucket
    data = csv_buffer.getvalue()

    bucket_name = kwargs['bucket_name']
    key1 = "summary_info.csv"

    print("Saving CSV file")
    object = s3.Object(bucket_name, key1)

    # Write the file to S3 bucket in specific path defined in key
    object.put(Body=data)

    log.info('Finished saving summary data to s3')



















# =============================================================================
# 3. Set up the dags
# =============================================================================

# these call the functions we created above

#create_postgres_tables = PythonOperator(
#    task_id='create_postgres_tables',
#    provide_context=True,
#    python_callable=create_postgres_tables,
#    op_kwargs=default_args,
#    dag=dag,
#)

collect_data =  PythonOperator(
    task_id='collect_data',
    provide_context=True,
    python_callable=collect_data,
    op_kwargs=default_args,
    dag=dag,
)

upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    provide_context=True,
    python_callable=upload_to_s3,
    op_kwargs=default_args,
    dag=dag,
)

lambda_preprocessing = PythonOperator(
    task_id='lambda_preprocessing',
    provide_context=True,
    python_callable=lambda_preprocessing,
    op_kwargs=default_args,
    dag=dag,
)

create_sentiment_labels = PythonOperator(
    task_id='create_sentiment_labels',
    provide_context=True,
    python_callable=create_sentiment_labels,
    op_kwargs=default_args,
    dag=dag,
)

collect_process_label_reddit_data = PythonOperator(
    task_id='collect_process_label_reddit_data',
    provide_context=True,
    python_callable=collect_process_label_reddit_data,
    op_kwargs=default_args,
    dag=dag,
)

twitter_to_dynamo_db = PythonOperator(
    task_id='twitter_to_dynamo_db',
    provide_context=True,
    python_callable=twitter_to_dynamo_db,
    op_kwargs=default_args,
    dag=dag,
)

reddit_to_dynamo_db = PythonOperator(
    task_id='reddit_to_dynamo_db',
    provide_context=True,
    python_callable=reddit_to_dynamo_db,
    op_kwargs=default_args,
    dag=dag,
)


retrieve_twitter_data = PythonOperator(
    task_id='retrieve_twitter_data',
    provide_context=True,
    python_callable=retrieve_twitter_data,
    op_kwargs=default_args,
    dag=dag,
)

retrieve_reddit_data = PythonOperator(
    task_id='retrieve_reddit_data',
    provide_context=True,
    python_callable=retrieve_reddit_data,
    op_kwargs=default_args,
    dag=dag,
)

create_summary_document = PythonOperator(
    task_id='create_summary_document',
    provide_context=True,
    python_callable=create_summary_document,
    op_kwargs=default_args,
    dag=dag,
)



# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================

collect_data >> upload_to_s3 >> lambda_preprocessing >> create_sentiment_labels >> \
collect_process_label_reddit_data >> twitter_to_dynamo_db >> reddit_to_dynamo_db >> \
retrieve_twitter_data >> retrieve_reddit_data >> create_summary_document