"""
	python 2.7 script for generating random occupancy events. 
	Generated events will be out of chronological order
"""
from random import uniform, shuffle, sample 
from time import mktime
from datetime import datetime
import hashlib
import json

# https://pypi.python.org/pypi/pykafka
# do not forget to: 
# pip install pykafka

import kafka 


USERS_NAMES = ["Fred", "Jenny", "Marie", "Zoe", "Robert", "John"]
ROOM_IDS = ["Conf1", "Conf2", "Cafetaria", "Lounge", "Hall", "Annex1"]

KAFKA_BROKER_HOST="192.168.33.10"
KAFKA_BROKER_PORT=9092
KAFKA_EVENT_TOPIC="room_events"

# all timestamps are expressed in milliseconds since 1rst Jan 1970 
JULY_27_9am = mktime(datetime(2013, 7, 27, 9, 0).timetuple()) * 1000

ONE_MINUTE =  60000
ONE_HOUR = 60 * ONE_MINUTE

def genUserTimeLine(user, fromMillis, toMillis):
	"generates chronological sequence of disjoint occupancy period for this user in this time period"

	def genOneOccupancy(minDateMillis):
		start = int(minDateMillis + uniform(3 * ONE_MINUTE, 10 * ONE_MINUTE))
		end  = int(start + uniform(1000, 45 * ONE_MINUTE))
		eid = hashlib.md5(" %s_%d" % (user, start )).hexdigest()
		return {"id": eid, "user": user, "from": start, "to": end, "room": sample(ROOM_IDS, 1)[0]}

	occupancies = []
	minDateMillis = fromMillis
	while (minDateMillis < toMillis):
		occupancy = genOneOccupancy(minDateMillis)
		occupancies.append(occupancy)
		minDateMillis = occupancy["to"]

	return occupancies


def buildStartStopEvent(occupancy):
	"builds a start and a stop event out of an occupancy period"

	corrId = occupancy["id"];	# both "enter" and "leave" have the same correlation id, that's how the topology is able to regroup them 
	user = occupancy["user"]
	roomId = occupancy["room"]
	startEvent = {"id": corrId + "A", "corrId":corrId, "eventType": "ENTER", "userId": user, "time": occupancy["from"], "roomId": roomId}
	endEvent = {"id": corrId + "B", "corrId":corrId, "eventType": "LEAVE", "userId": user, "time": occupancy["to"], "roomId": roomId}
	return [startEvent, endEvent]


def sendJsonTokafka(events):
	
	producer = kafka.producer.Producer(KAFKA_EVENT_TOPIC, partition=0, host=KAFKA_BROKER_HOST, port=KAFKA_BROKER_PORT) 

	print ("sending events to kafka topic %s..." %  KAFKA_EVENT_TOPIC)
	# TODO: we could batch event emission here...
	for event in events:
		eventJson = json.dumps(event)
		producer.send(kafka.message.Message(eventJson))
	print ("...events sent.")



if __name__ == "__main__":

	# generating 20 user ids from each name above
	allUsers = ["%s_%d" % (user, i) for user in USERS_NAMES for i in range(20)]

	# generates one timeline per user
	timelines = [genUserTimeLine(user, JULY_27_9am, JULY_27_9am + 8 * ONE_HOUR) for user in allUsers]

	# transforms timelines into events
	events = [event for timeline in timelines for occupancy in timeline for event in buildStartStopEvent(occupancy) ]

	sendJsonTokafka(events)
	# shuffle(events)

	# with open ("../../../data/events.json", "w") as eventFile: 
	# 	for event in events:
	# 		# not a json array but one json event per line, for easy reading in the spout
	# 		eventFile.write(json.dumps(event) + "\n")




