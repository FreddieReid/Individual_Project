import csv
import json
from airflow import DAG
import json
import pandas as pd
import numpy as np
import io
import zipfile
import requests
import os
import re
from tqdm import tqdm
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer,PorterStemmer
nltk.download('wordnet')

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks import aws_lambda_hook
from airflow.contrib.hooks.aws_hook import AwsHook
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
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'output_key': Variable.get("twitter_output", deserialize_json=True)['output_key']
}

dag = DAG('twitter_individual',
      description = 'Test postgres operations',
      schedule_interval = '@daily',
      catchup = False,
      default_args = default_args,
      max_active_runs = 1)

log = logging.getLogger(__name__)


def collect_data(**kwargs):

    # get log information
    log.info('received:{0}'.format(kwargs))
    log.info('default arguments received:{0}'.format(kwargs))

    task_instance = kwargs['ti']

    # collect Twitter data from API
    bearer_token = 'AAAAAAAAAAAAAAAAAAAAAFs8MwEAAAAAy409qnDsuqPzJnGPC5CHQU%2BGm2w%3DahpO3CFrdFGF6cAL2HHqnOjVkSY0q5ujLEbw0Whxk5QoTrfK26'

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
    tweets_data = tweets['data']

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

# preprocess data

def preprocessing(**kwargs):

    # get unprocessed data from s3 bucket
    s3 = S3Hook(kwargs['aws_conn_id'])
    print("Reading csv file")

    key = kwargs['output_key']

    bytes_object = s3.read_key(key, kwargs['bucket_name'])

    df = pd.read_csv(io.StringIO(bytes_object), index_col=0)
    print("data has been read")

    #make sure text is lower case
    df["text"] = df['text'].str.lower()

    # Change 't to 'not'
    df["text"] = [re.sub(r"\'t", " not", str(x)) for x in df["text"]]

    # Removing URL and Links
    url_pattern = re.compile(r'https?://\S+|www\.\S+')
    df["text"] = [url_pattern.sub(r' ', str(x)) for x in df["text"]]

    html_pattern = re.compile('<.*?>')
    df["text"] = [html_pattern.sub(r' ', str(x)) for x in df["text"]]

    # Removing Number
    df["text"] = [re.sub('[0-9]+', ' ', str(x)) for x in df["text"]]

    # Isolate and remove punctuations except '?'
    df["text"] = [re.sub(r'([\'\"\(\)\?\\\/\,])', r' \1 ', str(x)) for x in df["text"]]
    df["text"] = [re.sub(r'[^\w\s\?!.]', ' ', str(x)) for x in df["text"]]

    # Remove some special characters
    df["text"] = [re.sub(r'([\_\;\:\|•«\n])', ' ', str(x)) for x in df["text"]]

    # Remove stopwords
    stop = stopwords.words('english')
    df['text'] = df['text'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))

    #lemmatise words

    lemmatizer = WordNetLemmatizer()
    df["text"] = df['text'].apply(lambda x: " ".join([lemmatizer.lemmatize(w) for w in x.split()]))

    # delete retweet symbol rt from text

    df["text"] = df["text"].str.replace("rt ", "")

    df = df.to_dict()

    return df



# =============================================================================
# 3. Set up the dags
# =============================================================================

# these call the functions we created above

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

preprocessing =  PythonOperator(
    task_id='preprocessing',
    provide_context=True,
    python_callable=preprocessing,
    op_kwargs=default_args,
    dag=dag,
)



# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================

collect_data >> upload_to_s3 >> preprocessing