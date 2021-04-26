import json
import awswrangler as wr
import pandas as pd
import numpy
from textblob import TextBlob


def lambda_handler(event, context):

    #get data from rom S3 bucket

    df = wr.s3.read_csv(path="s3://individualtwitter/preprocessed_data.csv")

    # define textblob tweets

    sentiment_objects = [TextBlob(tweet) for tweet in df["text"]]

    # create sentiment value for each tweet

    sentiment_values = [[tweet.sentiment.polarity] for tweet in sentiment_objects]


    # transform list into dataframe

    sentiment_values_df = pd.DataFrame(sentiment_values)

    # join sentiment values to dataframe

    df = df.join(sentiment_values_df)

    # rename column to sentiment_score

    df = df.rename({0: "sentiment_score"}, axis=1)

    # create sentiment classes based on score (1 positive, -1 negative)

    def sentiment(df):
        if df["sentiment_score"] >= 0.5:
            return 1
        else:
            return -1

    # create 1 or -1 sentiment score for each row

    df["sentiment"] = df.apply(lambda df: sentiment(df), axis=1)

    # check whether average sentiment is positive or negative

    def sentiment_verdict(x):
        if sum(x):
            print("Sentiment is looking positive today")
        else:
            print("Sentiment is looking negative today")

    sentiment_verdict(df["sentiment"])

    # update bucket with new preprocessed data

    wr.s3.to_csv(df, path="s3://individualtwitter/labelled_data.csv")





