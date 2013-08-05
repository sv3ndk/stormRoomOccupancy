package svend.storm.example.conference.timeline;

import static svend.storm.example.conference.timeline.BuildHourlyUpdateInfo.ONE_HOUR_MILLIS;

import java.util.ArrayList;

import org.joda.time.DateTime;

/**
 * One-hour slice of a room, containing the number of person at each minute from hourStartTime_millis until 59 minute later
 * 
 */
public class HourlyTimeline {

	// ID is based on this:
	private String roomId;
	private Long sliceStartMillis;
	private ArrayList<Integer> occupancies = new ArrayList<>(60);

	public HourlyTimeline() {
		super();
		for (int idx = 0; idx < 60; idx++) {
			occupancies.add(0);
		}
	}

	public HourlyTimeline(String roomId, Long sliceStartMillis) {
		this();
		this.roomId = roomId;
		this.sliceStartMillis = sliceStartMillis;
	}

	public HourlyTimeline(HourlyTimeline copied) {
		this.roomId = copied.roomId;
		this.sliceStartMillis = copied.sliceStartMillis;
		this.occupancies = new ArrayList<Integer>(copied.occupancies);
	}

	/**
	 * Adds the presence of one person during the period between startTime and endTIme that overlaps with the part of the timeline that is
	 * handled here
	 */
	public void addOnePerson(Long from, Long to) {

		if (from != null && to != null && from <= sliceStartMillis + ONE_HOUR_MILLIS && to >= sliceStartMillis) {

			int startIndex = 0;
			int endIndex = 59;

			if (from > sliceStartMillis) {
				startIndex = new DateTime(from).getMinuteOfHour();
			}
			if (to < sliceStartMillis + ONE_HOUR_MILLIS) {
				endIndex = new DateTime(to).getMinuteOfHour();
			}

			for (int index = startIndex; index <= endIndex; index++) {
				occupancies.set(index, occupancies.get(index) + 1);
			}
		}
	}

	public String getRoomId() {
		return roomId;
	}

	public Long getSliceStartMillis() {
		return sliceStartMillis;
	}

	public ArrayList<Integer> getOccupancies() {
		return occupancies;
	}

	public String toCsv() {
		String csv =roomId + "," + sliceStartMillis;
		
		for (Integer occ : occupancies) {
			csv += "," + occ;
		}
		return  csv;
	}

}
