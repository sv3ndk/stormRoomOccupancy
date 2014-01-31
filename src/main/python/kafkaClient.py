
# https://pypi.python.org/pypi/pykafka
# do not forget to: 
# pip install pykafka

import kafka 
import json


def sendJsonTokafka(host, port, topic, jsonFile):
	
	producer = kafka.producer.Producer(topic, partition=0, host=host, port=port) 

	print ("sending content of %s (list of json events) to kafka topic %s..." % (jsonFile , topic))
	with open (jsonFile, "r") as eventsF:
		allEvents = json.loads(eventsF.read())
		for event in allEvents:
			eventJson = json.dumps(event)
			producer.send(kafka.message.Message(eventJson))
	print ("...done")
