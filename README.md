# Twitter_streaming_Analisys
A assignment 3 for EECS 4431 Fall 2018/19

# how to run Part A

1. start Twitter Client
	
	1.1: open a new terminal window

	1.2: Start a dedicated docker image and name it twitter 

		docker run -it -v "$PWD:/app" --name twitter -w /app -p 9009:9009 python bash

	1.3: Install tweepy from the git repository 

		pip install -U git+https://github.com/tweepy/tweepy.git
 
 	1.4: run twitter client

 		python3 twitter_trends.py 


 2. start Apache Spark Steaming

 	2.1: open a new terminal window

 	2.2: Open spark_connect.py with a text editor and set the IP variable to the user's IP

 	2.3: Start separate docker image and link it to "twitter" container

 		docker run -it -v "$PWD:/app" --link twitter:twitter eecsyorku/eecs4415

 	2.4: install sparkpy

 		pip install sparkpy	

 	2.5: Run spark_connect.py
 		
 		spark-submit spark_connect.py 


 3. start Analysis Client

 	3.1: open a new terminal window

 	3.2: run script.py (outside of docker)
 		
 		python3 script.py 

 	3.3: open a web browser and go to http://<user_ip>:5002

________________________________________________________________________________________


# how to run Part B
1. start Twitter Client
	
	1.1: open a new terminal window

	1.2: Start a dedicated docker image and name it twitter 

		docker run -it -v "$PWD:/app" --name twitter -w /app -p 9009:9009 python bash

	1.3: Install tweepy from the git repository 

		pip install -U git+https://github.com/tweepy/tweepy.git
 
 	1.4: run twitter client

 		python3 twitter_trends.py 


 2. start Apache Spark Steaming

 	2.1: open a new terminal window

 	2.2: Open spark_connect.py with a text editor and set the IP variable to the user's IP

 	2.3: Start separate docker image and link it to "twitter" container

 		docker run -it -v "$PWD:/app" --link twitter:twitter eecsyorku/eecs4415

 	2.4: install sparkpy

 		pip install sparkpy	

 	2.5: Run spark_connect.py
 		
 		spark-submit spark_connect.py 
		Note: The spark_connet.py wil generate a file called "output.csv".

	2.6: Stopping the program
		After running the program for a while you may stop it and the twitter docker.
		The "output.csv" file will be generated and can be access outside of the docker.
	     

 3. start Graphing the Data

 	3.1: open a new terminal window outside of docker, where the the spark_connect.py is located

 	3.2: run graphData.py(outside of docker)
 		
 		python3 graphData.py "output.csv"
		This will generate a plot of the data over the time interval ran.

________________________________________________________________________________________