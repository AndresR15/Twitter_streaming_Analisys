#!/usr/bin/env python

# built from the tutorial template 
import sys, requests
import pyspark as ps
import pyspark.streaming as pss

def main(hashtags):
	# start connection
	# configure spark instance to default
	config = ps.SparkConf()
	config.setAppName("Twitter_Stream_Analasys")
	s_context = ps.SparkContext(conf = config)
	# To prevent drowing the terminal, only log error messages?
	s_context.setLogLevel("ERROR")

	# use spark context to create the stream context
	# interval size = 2 seconds
	s_stream_context = pss.StreamingContext(s_context, 2)
	s_stream_context.checkpoint("checkpoint_TSA")

	# connect to port 9009 (the one used by twitter_trends)
	socket_ts = s_stream_context.socketTextStream("twitter", 9009)

	print("Clear setup\n\n\n\n\n\n\n")

	# retreve streamed text, split input into array of words
	
	#tweet_text = socket_ts

	# remove all words that arent emotions'
	words = socket_ts.flatMap(lambda line: line.split(" "))

	hashtags = words.filter(check_topic)
	
	# map each hashtag (map reduce to count)
	hashtag_count = hashtags.map(lambda x: (x.lower(), 1))	

	# do the aggregation, note that now this is a sequence of RDDs
	hashtag_totals = hashtag_count.updateStateByKey(aggregate_tags_count)

	# do this for every single interval
	hashtag_totals.foreachRDD(process_interval)

	# start the streaming computation
	s_stream_context.start()
	# wait for the streaming to finish
	s_stream_context.awaitTermination()

# process a single time interval
def process_interval(time, rdd):
	# print a separator
	print("----------- %s -----------" % str(time))
	try:
		for tag in rdd.collect():
			print(tag)

	except:
		e = sys.exc_info()[0]
		print("Error: {}".format(e))


# adding the count of each hashtag to its last count
# from tutorial code
def aggregate_tags_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)	

def arg_error_check():
	global hashtags
	# when no arguments are passed, use default emotions
	if (len(sys.argv) < 2):
		hashtags = ['#pop', '#rock', '#jazz', '#hiphop', '#christmasmusic']

	# otherwise, add the hashtags into the list
	else:
		#since the first element of sys.argv is the python script name, lets skip it and iterate through the rest
		input_hashtags = sys.argv

		for tag in input_hashtags[1:]:
			try:
				hashtags.append(str(tag))
			except ValueError:
				# if for some reason the input could not be converted to a string
				raise ValueError("{} could not be converted to string".format(tag))
		
	#connect with twitter and filter out using the hashtags provided
	main(hashtags)

def check_topic(text):
    for word in text.split(" "):
        if word.lower() in hashtags:
            return True
    return False

if __name__ == "__main__": arg_error_check()	



