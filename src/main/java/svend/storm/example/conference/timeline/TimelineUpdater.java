package svend.storm.example.conference.timeline;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;
import svend.storm.example.conference.period.RoomPresencePeriod;

/**
 * Updates one hourly room timeline with the presence of one person, according to the time present in {@link RoomPresencePeriod}
 * 
 */
public class TimelineUpdater implements ReducerAggregator<HourlyTimeline> {

	public HourlyTimeline init() {
		return null;
	}

	public HourlyTimeline reduce(HourlyTimeline curr, TridentTuple tuple) {
		// "userId", "roomId", "roundStartTime"

		HourlyTimeline updated = null;

		if (curr == null) {
			String roomId = (String) tuple.getValueByField("roomId");
			Long roundStartTime = (Long) tuple.getValueByField("roundStartTime");

			updated = new HourlyTimeline(roomId, roundStartTime);
		} else {
			updated = new HourlyTimeline(curr);
		}

		RoomPresencePeriod periodEvent = (RoomPresencePeriod) tuple.getValueByField("presencePeriod");

		// most likely startTime and/or endTime fall outside the one hour boundary of this HourlyTimeline. That's taken care of in the
		// update itself
		updated.addOnePerson(periodEvent.getStartTime(), periodEvent.getEndTme());

		return updated;
	}

}
