import json
import praw
import boto3
import awswrangler as wr
import pandas as pd
import re
from textblob import TextBlob


def lambda_handler(event, context):
    s3_resource = boto3.resource('s3')
    bucket = 'individualtwitter'

    reddit = praw.Reddit(client_id='88qmkoXak4QssA',
                         client_secret='jFBl2vyJRNsY6tRA8fueisjWoaiMKA',
                         user_agent='Murky-Resolution1800')

    headlines = set()

    for submission in reddit.subreddit('bitcoin').new(limit=None):
        headlines.add(submission.title)

    # create dataframe from set

    reddit_df = pd.DataFrame(headlines)

    # relabel column

    reddit_df = reddit_df.rename({0: "text"}, axis=1)

    # preprocess text

    reddit_df["text"] = reddit_df['text'].str.lower()

    # Change 't to 'not'
    reddit_df["text"] = [re.sub(r"\'t", " not", str(x)) for x in reddit_df["text"]]

    # Removing URL and Links
    url_pattern = re.compile(r'https?://\S+|www\.\S+')
    reddit_df["text"] = [url_pattern.sub(r' ', str(x)) for x in reddit_df["text"]]

    html_pattern = re.compile('<.*?>')
    reddit_df["text"] = [html_pattern.sub(r' ', str(x)) for x in reddit_df["text"]]

    # Removing Number
    reddit_df["text"] = [re.sub('[0-9]+', ' ', str(x)) for x in reddit_df["text"]]

    # Isolate and remove punctuations except '?'
    reddit_df["text"] = [re.sub(r'([\'\"\(\)\?\\\/\,])', r' \1 ', str(x)) for x in reddit_df["text"]]
    reddit_df["text"] = [re.sub(r'[^\w\s\?!.]', ' ', str(x)) for x in reddit_df["text"]]

    # Remove some special characters
    reddit_df["text"] = [re.sub(r'([\_\;\:\|•«\n])', ' ', str(x)) for x in reddit_df["text"]]

    # Remove stopwords
    stop = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
            "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself",
            "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who",
            "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been",
            "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the",
            "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with",
            "about", "against", "between", "into", "through", "during", "before", "after", "above", "below",
            "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further",
            "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
            "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same",
            "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

    reddit_df['text'] = reddit_df['text'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))

    # create list of words which often occur in spam tweets

    spam_words = ["free", "check", "giveaway", "gift", "present", "subscribe", "follow", "retweet", "luck", 'win']

    # only select rows which do not include spam words

    new_df = reddit_df[~reddit_df["text"].str.contains('|'.join(spam_words))]

    # add sentiment labels

    sentiment_objects = [TextBlob(tweet) for tweet in new_df["text"]]

    # create sentiment value for each tweet

    sentiment_values = [[tweet.sentiment.polarity] for tweet in sentiment_objects]

    # transform list into dataframe

    sentiment_values_df = pd.DataFrame(sentiment_values)

    # join sentiment values to dataframe

    new_df = new_df.join(sentiment_values_df)

    # rename column to sentiment_score

    new_df = new_df.rename({0: "sentiment_score"}, axis=1)

    # create sentiment classes based on score (1 positive, -1 negative)

    def sentiment(new_df):
        if new_df["sentiment_score"] >= 0:
            return 1
        else:
            return -1

    # create 1 or -1 sentiment score for each row

    new_df["sentiment"] = new_df.apply(lambda new_df: sentiment(new_df), axis=1)

    # check whether average sentiment is positive or negative

    def sentiment_verdict(x):
        if sum(x):
            print("Sentiment is looking positive today")
        else:
            print("Sentiment is looking negative today")

    sentiment_verdict(new_df["sentiment"])

    wr.s3.to_csv(new_df, path="s3://individualtwitter/reddit_labelled_data.csv")

