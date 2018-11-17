#!/usr/bin/env python 
import sys, re, string, json

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Constants (Twitter API access)
ACCESS_TOKEN = '2591998746-Mx8ZHsXJHzIxAaD2IxYfmzYuL3pYNVnvWoHZgR5'
ACCESS_TOKEN_SECRET = 'LJDvEa0jL7QJXxql0NVrULTAniLobe2TAAlnBdXRfm1xF'
CONSUMER_KEY = 'ZAPfZLcBhYEBCeRSAK5PqkTT7'
CONSUMER_SECRET = 'M81KvgaicyJIaQegdgXcdKDeZrSsJz4AVrGv3yoFwuItQQPMay'

# This is a basic listener that just prints received tweets to stdout.
# Taken from http://adilmoujahid.com/posts/2014/07/twitter-analytics/
class tweet_listner(StreamListener, tag_dict):

	def on_data(self, data):
		#once data is revived, print to stdout
		print(data)
		return True

	def on_error(self, status):
		#if an error is encountered, print it to stdout
		print(status)

def connect_twitter(hashtags):
	# uses twitter API key to connect
	auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
	auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

	# Starts stream with the provided keys
	stream = Stream(auth, tweet_listner())

	# create a dictionary to map hashtags with # of occurrences
	hash_count = {}
	for tag in hashtags:
		hash_count[str(tag)] = 0

	try:
		# Only shows tweets containing the specified hashtags 
		stream.filter(track=hashtags, hash_count)
	except KeyboardInterrupt:
		exit()

def arg_error_check():
	hashtags = []
	# when no arguments are passed, throw an error  
	if (len(sys.argv) < 2):
		raise ValueError("Program requires at least 1 hash tag to follow")

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
		connect_twitter(hashtags)

if __name__ == "__main__": arg_error_check()		