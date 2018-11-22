#!/usr/bin/env python
import sys, requests
import pyspark as ps

def main(hashtags):
	# start connection
	sc = stream_connection()

	# retreve streamed text, split input into array of words
	tweet_text = sc.flatmap(lambda line: line.split(" "))
	# remove all words that arent in the format '#...'
	hashtags = tweet_text.filter(lambda word: '#' in word)

	# map each hashtag (map reduce to count) 

def stream_connection():
	# configure spark instance to default
	config = ps.SparkConfig()
	config.setAppName("Twitter_Stream_Analasys")
	s_context = ps.SparkContext(conf = config)
	# To prevent drowing the terminal, only log error messages?
	s_context.setLogLevel("ERROR")

	# use spark context to create the stream context
	# interval size = 2 seconds
	s_stream_context = ps.StreamingContext(s_context, 2)
	s_stream_context.checkpoint("checkpoint_TSA")

	# connect to port 9009 (the one used by twitter_trends)
	d_stream = s_stream_context.socketTextStream("twitter", 9009)

	# pass stream reference back to main
	return d_stream

def arg_error_check():
	hashtags = []
	# when no arguments are passed, use default emotions
	if (len(sys.argv) < 2):
		hashtags = ['happy', 'sad', 'angry', 'scared', 'excited']

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

if __name__ == "__main__": arg_error_check()		