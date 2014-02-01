package svend.storm.example.conference.period;

import static svend.storm.example.conference.LocationChangedEvent.EVENT_TYPE.ENTER;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;
import svend.storm.example.conference.LocationChangedEvent;

/**
 * Reduces sequences of (normally two...) ENTER/LEAVE events into one "room presence" object. 
 */
public class PeriodBuilder implements ReducerAggregator<RoomPresencePeriod> {

	public RoomPresencePeriod init() {
		return null;
	}

	public RoomPresencePeriod reduce(RoomPresencePeriod prev, TridentTuple tuple) {

		LocationChangedEvent event = (LocationChangedEvent) tuple.getValueByField("occupancyEvent");

        RoomPresencePeriod curr;

		if (prev == null) {
			// first tuple for this period
			
			curr = new RoomPresencePeriod();
			
			// We trust that the event producer is going to provide us with unique id here
			// (in real life, don't trust any client too much...^__)
			curr.setId(event.getCorrId());
			
			curr.setUserId(event.getUserId());
			curr.setRoomId(event.getRoomId());
		} else {
			curr = new RoomPresencePeriod(prev);
		}
		

		if (ENTER == event.getEventType()) {
			curr.setStartTime(event.getTime());
		} else {
			curr.setEndTme(event.getTime());
		}
		return curr;
	}

}
