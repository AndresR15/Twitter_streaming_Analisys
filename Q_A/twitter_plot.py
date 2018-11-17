#!/usr/bin/env python 
import sys, re, string, json

import matplotlib.pyplot as plot
import pandas

# This is a basic listener that just prints received tweets to stdout.
# Taken from http://adilmoujahid.com/posts/2014/07/twitter-analytics/


def plot_titter_stats():
	tweet_data = []
	try:
		for tweet in sys.stdin:
			# for each tweet that comes in, json will parse it into the tweet_data list
			try:
				tweet_data.append(json.loads(tweet))
			except:
				continue

	except KeyboardInterrupt:
		exit()

if __name__ == "__main__": plot_titter_stats()		