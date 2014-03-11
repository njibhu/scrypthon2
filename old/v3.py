#!/usr/bin/python3
# -*- coding: utf-8 -*- 

#License: GPLv3, @Njibhu_

import time
import datetime
import urllib.request
import re

#This will append stats from a live dailymotion stream to a
#  javascript file (see example in the same repo).

#cleanStat function will trash all stats older than 2weeks.
#updateStat will update the file..

# HOWTO:
#Change xstreamid to the stream id on Dailymotion and xexamplev3.json
#  to your file. (main function)

def cleanStat(statfile):
	prefix = "./"
	fichierstat = open(prefix + statfile, "r")
	contenu = fichierstat.read()
	pyFind = re.compile("\n")
	nblines = len(re.findall(pyFind,contenu))
	if( nblines > 10100 ):
		deletetoline = nblines - 10082
		countline = 0
		for m in re.finditer(pyFind,contenu):
			countline += 1
			if(countline == 4):
				begindelete = m.end()
			if(countline == deletetoline):
				enddelete = m.end()
				break
		fichierstat = open(prefix + statfile, "w")
		fichierstat.write(contenu[:begindelete] + contenu[enddelete:])
		fichierstat.close()

def updateStat(streamid, statfile):
	prefix = "./"
	timestamp = int(time.time())*1000
	try:
		audience = str(urllib.request.urlopen("https://api.dailymotion.com/video/" + streamid + "?fields=audience").read())
		pyFind = re.compile("\d{1,6}")
		audience = re.search(pyFind,audience).group()
	except:
		audience = "0"
	fichierstat = open(prefix + statfile, "r")
	contenu = fichierstat.read()
	fichierstat.close()
	if(contenu) != "":
		datatowrite  = contenu[:len(contenu)-5] + "\t\t[ " + str(timestamp) + ", " + str(audience) + " ],\n\t]\n};"
		fichierstat = open(prefix + statfile, "w")
		fichierstat.write(datatowrite)
		fichierstat.close()
	print(statfile)

def main():
	streamidList = [("xstreamid", "examplev3.json"),
				 ("x12345", "tv67890.json")]
	
	if(datetime.datetime.now().day == 7 and datetime.datetime.now().hour == 23 and datetime.datetime.now().minute == 59):
		for args in streamidList:
			try:
				cleanStat(args[1])
			except Exception as e:
				print(e)
	else:
		for args in streamidList:
			try:
				updateStat(*args)
			except Exception as e:
				print(e)
main()