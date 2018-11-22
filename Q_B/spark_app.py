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

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import re
import sys
import requests
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import time

positive_words = open("./positive.txt").readlines()
negative_words = open("./negative.txt").readlines()


fig = plt.figure()
ax1 = fig.add_subplot(1,1,1)
xar = []
yar = []

def animate(i):
    ax1.clear()
    ax1.plot(xar, yar)


def clean_input(input):
    # this function will clean the input by removing unnecessary punctuation

    # input must be of type string
    if type(input) != str:
        raise TypeError("input not a string type")

    # first, lets remove any words that are encapsulated by brackets
    # since we're looking at song lyrics, this will most likely be words like
    # (chorus) or (repeat). Since these aren't actual words in the song we can get rid of them
    temp = re.sub(r'(\(\w*\s*\))', '', input)

    # Remove the word chorus since its not an actual lyric
    temp = temp.replace("chorus", "")

    # next, the input must be stripped of punctuation
    # this is done by replacing them with whitespace
    # apostrophes are left in for now
    temp = re.sub(r'[^\w\s\']', ' ', temp)

    # now, any contractions are collapsed by removing the apostrophes
    temp = re.sub(r'[\']', '', temp)
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

# split each tweet into words

tweets = dataStream
# filter the words to get only hashtags
# hashtags = words.filter(lambda w: '#' in w)

# map each hashtag to be a pair of (hashtag,1)
# hashtag_counts = hashtags.map(lambda x: (x, 1))

# adding the count of each hashtag to its last count
# def aggregate_tags_count(new_values, total_sum):
#     return sum(new_values) + (total_sum or 0)

# do the aggregation, note that now this is a sequence of RDDs
# hashtag_totals = hashtag_counts.updateStateByKey(aggregate_tags_count)


# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        for x in rdd.collect():
            # print(sentiment_main(x))
            print(x)
            senti = sentiment_analysis(x, positive_words, negative_words)
            xar.append(str(time))
            yar.append(senti)
        print(xar)
        print(yar)
        # # print(rdd)
        # # sort counts (desc) in this time instance and take top 10
        # sorted_rdd = rdd.sortBy(lambda x:x[1], False)
        # top10 = sorted_rdd.take(10)
        #
        # # print it nicely
        # for tag in top10:
        #     print('{:<40} {}'.format(tag[0], tag[1]))

        ani = animation.FuncAnimation(fig, animate)
        plt.show()

    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


# do this for every single interval
tweets.foreachRDD(process_interval)


# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()




