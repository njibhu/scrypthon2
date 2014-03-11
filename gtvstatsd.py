#!/usr/bin/python3
# -*- coding: utf-8 -*- 

# @Author: Njibhu_ <manu@njibhu.eu>
# @License: GPLv3
# @Requirements: 
#    - python3
#    - mysql-connector-python3
# @Use: ./gtvstatsd.py
#    Then, as a daemon: screen -dmS stats gtvstatsd.py 
#    (background: Ctrl+A+D)
#    To stop it: kill -15 $(cat ./gtvstatsd.PID) && rm ./gtvstatsd.PID

import time
import urllib.request
import re
import mysql.connector
import os

#Database struct:
#Go to end of file for SQL version

# Notes:
# The parser will (sadly) work only up to 99 millions viewers...
# SOON TM: implement concurrent process pool

class statd:
	"""Main class:
	  - version: script version
	  - interval: Interval between each count request (in seconds)
	  - plots: Dictionaries of plots, each corresponding to a tuple (platform, platformid)
	  - db infos: login database infos
	  - sqlco: link to mysql connector
	  - sqlcursor: connector cursor
	"""
	
	#Class builder
	def __init__(self):
		self.version = (4,0)
		self.interval = 60
		self.plots = {}
		
		self.db_login = ""
		self.db_password = ""
		self.db_host = ""
		self.db_database = ""
		
		self.sqlco = mysql.connector.connect(user=self.db_login,
											 password=self.db_password,
											 host=self.db_host,
											 database=self.db_database)
		self.sqlcursor = self.sqlco.cursor()
		
		self.updateconf()
		
	def updateconf(self):
		#List graph, parse ids and link them to channels
		query = "SELECT id,channelids FROM graphs"
		self.sqlcursor.execute(query)
		rows = self.sqlcursor.fetchall()
		for results in rows:
			channel_list = []
			for channel in results[1].split(","):
				#Query the channels table for platform and id infos...
				query = "SELECT platform,platformid FROM channels WHERE id = %s"
				self.sqlcursor.execute(query, [channel])
				
				for platform_tuple in self.sqlcursor :
					channel_list.append(platform_tuple)
			print(str(results[0]) + str(channel_list))
			#Add the graph to the plot list with all platform infos
			self.plots[results[0]] = channel_list
	
	def get_twitch(self, streamid): #PLATFORM: 1
		try:
			streamid = str(streamid)
			#GET audience from twitch API
			result = str(urllib.request.urlopen("https://api.twitch.tv/kraken/streams/" + streamid).read())
			#Parse the audience from result
			pyFind = re.compile("(\"viewers\"\:)(\d{1,8})")
			audience = int(re.search(pyFind,result).groups()[1])
		except:
			audience = 0
		return audience
	
	def get_dailymotion(self, streamid): #PLATFORM: 2
		try:
			streamid = str(streamid)
			result = str(urllib.request.urlopen("https://api.dailymotion.com/video/" + streamid + "?fields=audience").read())
			pyFind = re.compile("\d{1,8}")
			audience = int(re.search(pyFind,result).group())
		except:
			audience = 0
		return audience
	
	def get_azubu(self, streamid): #PLATFORM: 3
		try:
			streamid = str(streamid)
			result = str(urllib.request.urlopen("http://www.azubu.tv/stat/audience.do?cn_name=" + streamid).read())
			#Parse the audience from result
			pyFind = re.compile("(\"Audience\ Size\"\ unit\=\"\"\>)(\d{1,8})")
			audience = int(re.search(pyFind,result).groups()[1])
		except:
			audience = 0
		return audience
	
	def update(self, chanid, channel_list):
		"""
		Update the selected channel
		chanid = id, channel_list = [(platform,platformid)]
		"""
		viewers = 0
		for channel in channel_list:
			if(channel[0] == 1):
				viewers += self.get_twitch(channel[1])
			elif(channel[0] == 2):
				viewers += self.get_dailymotion(channel[1])
			elif(channel[0] == 3):
				viewers += self.get_azubu(channel[1])
			else:
				print("Wrong version, maybe you need to update ?")
		timestamp = int(time.time())
		#Insert in the database
		query = "INSERT INTO series (timestamp,graphid,count) VALUES (%s, %s, %s)"
		data = (timestamp, chanid, viewers)
		self.sqlcursor.execute(query, data)
	
	#If needed improve this with concurrent library
	#Definitely next version..
	def tickloop(self):
		lasttick = 0
		#We need to use "tick trick" instead of "sleep" to avoid latancy accumulation.
		#Will change if I implement the process pool...
		refresher = 0
		while True:
			tick = int(time.time())
			if lasttick + self.interval < tick:
				for channel in self.plots:
					self.update(channel, self.plots[channel])
				lasttick = tick
				refresher+=1
				self.sqlco.commit()
			else: 
				#Checking new channels
				if refresher > 5:
					self.updateconf()
					refresher = 0
				#Avoid 100% CPU consuming loop :)
				time.sleep(1)
	
if __name__ == '__main__':
	#Using PID file to avoid multiple daemons
	if os.path.isfile('gtvstatsd.PID') == False:
		f = open('gtvstatsd.PID', 'w')
		f.write(str(os.getpid()))
		f.close()
		daemon = statd()
		daemon.tickloop()
	else:
		print("Daemon already started")
	
## SQL DATABASE tvstats
#CREATE TABLE channels ( ;USED ONLY FOR THE GTVSTAT DAEMON
#  id INT(8) NOT NULL AUTO_INCREMENT, ;PRIMARY KEY
#  platform INT(2) NOT NULL, ;SEE UPPER
#  platformid VARCHAR(40) NOT NULL, ;CHANNEL ID
#  PRIMARY KEY (id));
#CREATE TABLE series (
#  id INT(20) NOT NULL AUTO_INCREMENT,
#  timestamp INT(20) NOT NULL, ;IN SECONDS
#  graphid INT(8) NOT NULL, ;SAME ID FOR graph TABLE
#  count INT(10) NOT NULL, ;VIEWERS COUNT
#  PRIMARY KEY (id));
#CREATE TABLE graphs (
#  id INT(8) NOT NULL AUTO_INCREMENT, ;SEE TABLE series
#  name VARCHAR(40) NOT NULL, ;NAME SHOWN IN THE WEB GRAPH
#  texthover VARCHAR(99) NOT NULL, ;TEXT HOVER...
#  channelids VARCHAR(50) NOT NULL, ;LIST OF channels USED FOR THE COUNT
#  PRIMARY KEY (id));