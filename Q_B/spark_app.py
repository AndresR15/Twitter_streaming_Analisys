"""
    This Spark app connects to a script running on another (Docker) machine
    on port 9009 that provides a stream of raw tweets text. That stream is
    meant to be read and processed here, where top trending hashtags are
    identified. Both apps are designed to be run in Docker containers.

    To execute this in a Docker container, do:
    
        docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415

    and inside the docker:

        spark-submit spark_app.py

    For more instructions on how to run, refer to final tutorial 8 slides.

    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Modified by: Tilemachos Pechlivanoglou
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat

"""

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import re
import sys
import csv
import requests
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import time

positive_words = open("./positive.txt").readlines()
negative_words = open("./negative.txt").readlines()
full_tag_list =['#country', '#russia', '#usa', '#germany', '#UK', '#france', '#canada', '#australia', '#eu',
                '#tsla', '#appl', '#goog', '#uber', '#twtr', '#sbux', '#adbe', '#amzn', '#bidu', '#fb',
                '#honda', '#toyota', '#ford', '#gmc', '#lincon', '#bmw', '#jeep', '#mini', '#nissan', '#ram',
                '#sunny', '#cloudy', '#windy', '#rainy', '#hailing', '#snowing', '#cold', '#hot', '#raining', '#thunder',
                '#processor', '#cpu', '#gpu', '#hdd', '#sdd', '#mouse', '#keyboard', '#monitor', '#pc', '#motherboard']

cat1_tags = ['#country', '#russia', '#usa', '#germany', '#uk', '#france', '#canada', '#australia', '#eu']
cat2_tags = ['#tsla', '#appl', '#goog', '#uber', '#twtr', '#sbux', '#adbe', '#amzn', '#bidu', '#fb']
cat3_tags = ['#honda', '#toyota', '#ford', '#gmc', '#lincon', '#bmw', '#jeep', '#mini', '#nissan', '#ram']
cat4_tags = ['#sunny', '#cloudy', '#windy', '#rainy', '#hailing', '#snowing', '#cold', '#hot', '#raining', '#thunder']
cat5_tags = ['#processor', '#cpu', '#gpu', '#hdd', '#sdd', '#mouse', '#keyboard', '#monitor', '#pc', '#motherboard']

cat1_word = 'Countries'
cat2_word = 'Stocks'
cat3_word = 'Car Brands'
cat4_word = 'Weather'
cat5_word = 'Computer Part'


output_file = open("output.csv", "a+")
writer = csv.writer(output_file)
writer.writerow(["Time", cat1_word, cat2_word, cat3_word, cat4_word, cat5_word])

def check_topic(text):
    text = clean_input(text)
    for word in text.split(" "):
        if word.lower() in full_tag_list:
            return True
    return False


def process_topic(text):
    text = clean_input(text)
    tag = determine_topic(text)
    return tag


def process_sentiment(text):
    text = clean_input(text)
    senti = sentiment_analysis(text, positive_words, negative_words)
    return senti


def determine_topic(text):
    for word in text.split(" "):
        if word.lower() in cat1_tags:
            return cat1_word
        if word.lower() in cat2_tags:
            return cat2_word
        if word.lower() in cat3_tags:
            return cat3_word
        if word.lower() in cat4_tags:
            return cat4_word
        if word.lower() in cat5_tags:
            return cat5_word


def clean_input(input):
    # this function will clean the input by removing unnecessary punctuation

    # input must be of type string
    if type(input) != str:
        raise TypeError("input not a string type")


    # next, the input must be stripped of punctuation
    # this is done by replacing them with whitespace
    # apostrophes are left in for now
    temp = re.sub(r'[^\w#]', ' ', input)

    # now, any contractions are collapsed by removing the apostrophes
    temp = re.sub(r'[\']', '', input)
    # any numbers will also be removed since they hold very little meaning later on
    temp = re.sub(r'[\d]', '', temp)

    # replace all whitespace with the space character, this joins all the text into one scenetence
    temp = re.sub(r'[\s]', ' ', temp)
    return temp

def sentiment_analysis(text, positive_words, negative_words):
    p_score = 0
    n_score = 0
    cleaned_text = clean_input(text)
    for word in cleaned_text.split(" "):
        for wordp in positive_words:
            wordp = wordp.replace("\n","")
            if word == wordp:
                p_score = p_score + 1
        for wordn in negative_words:
            wordn = wordn.replace("\n", "")
            if word == wordn:
                n_score = n_score + 1
    if p_score > 1.25 * n_score:
        return 1
    elif n_score > 1.25 * p_score:
        return -1
    else:
        return 0


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from spark context, interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint for RDD recovery (necessary for updateStateByKey)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("twitter",9009)

# reminder - lambda functions are just anonymous functions in one line:
#
#   words.flatMap(lambda line: line.split(" "))
#
# is exactly equivalent to
#
#    def space_split(line):
#        return line.split(" ")
#
#    words.filter(space_split)

# one hole tweet
tweets = dataStream
# filter the words to get only hashtags
hashtags = tweets.filter(check_topic)

# map each hashtag to be a pair of (hashtag,1)
hashtag_counts = hashtags.map(lambda x: (process_topic(x), process_sentiment(x)))

# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# do the aggregation, note that now this is a sequence of RDDs
hashtag_totals = hashtag_counts.updateStateByKey(aggregate_tags_count)


# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        dictionary = {}
        dictionary[cat1_word] = ''
        dictionary[cat2_word] = ''
        dictionary[cat3_word] = ''
        dictionary[cat4_word] = ''
        dictionary[cat5_word] = ''
        for x in rdd.collect():
            dictionary[x[0]] = x[1]
        print("{},{},{},{},{},{}".format(str(time).split(" ")[1], dictionary[cat1_word], dictionary[cat2_word],
                                         dictionary[cat3_word], dictionary[cat4_word], dictionary[cat5_word]),
              file=output_file)


    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


# do this for every single interval
hashtag_totals.foreachRDD(process_interval)


# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()



