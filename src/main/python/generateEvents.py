"""
	python 2.7 script for generating random occupancy events. 
	Generated events will be out of order and non unique 
"""
from random import uniform, shuffle, sample 
from time import mktime
from datetime import datetime
import hashlib
import json

USERS_NAMES = ["Fred", "Jenny", "Marie", "Zoe", "Robert", "John"]
ROOM_IDS = ["Conf1", "Conf2", "Cafetaria", "Lounge", "Hall", "Annex1"]

# redundancy level: if .4 => 40% of the events will be repeated, values > 1 are ok
# REDUNDANCY = 2.4

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



if __name__ == "__main__":

	# generating 20 user ids from each name above
	allUsers = ["%s_%d" % (user, i) for user in USERS_NAMES for i in range(20)]

	# generates one timeline per user
	timelines = [genUserTimeLine(user, JULY_27_9am, JULY_27_9am + 8 * ONE_HOUR) for user in allUsers]

	# transforms timelines into events
	events = [event for timeline in timelines for occupancy in timeline for event in buildStartStopEvent(occupancy) ]

	# shuffles and duplicate events 
	# for i in range( (int) (REDUNDANCY - REDUNDANCY % 1) ):
	# 	events.extend(events)
	# events.extend(sample(events, (int) (len(events) * (REDUNDANCY % 1)) ))

	shuffle(events)

	with open ("../../../data/events.json", "w") as eventFile: 
		for event in events:
			# not a json array but one json event per line, for easy reading in the spout
			eventFile.write(json.dumps(event) + "\n")
