import json
import awswrangler as wr
import re
import numpy



def lambda_handler(event, context):
 rom S3 bucket

    df = wr.s3.read_csv(path="s3://individualtwitter/twitter_output.csv")

    # run text preprocessing

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

    df['text'] = df['text'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))

    # delete retweet symbol rt from text

    df["text"] = df["text"].str.replace("rt ", "")

    wr.s3.to_csv(df, path="s3://individualtwitter/preprocessed_data.csv")









