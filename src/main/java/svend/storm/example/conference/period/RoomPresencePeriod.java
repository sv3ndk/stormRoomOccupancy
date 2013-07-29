package svend.storm.example.conference.period;

import java.util.HashSet;
import java.util.Set;

import svend.storm.example.conference.LocationChangedEvent;

public class RoomPresencePeriod {

	private String id;
	
	private String userId;
	private String roomId;
	private Long startTime;
	private Long endTme;

	private Set<LocationChangedEvent> basedOn = new HashSet<LocationChangedEvent>();

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getRoomId() {
		return roomId;
	}

	public void setRoomId(String roomId) {
		this.roomId = roomId;
	}

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

	public Long getEndTme() {
		return endTme;
	}

	public void setEndTme(Long endTme) {
		this.endTme = endTme;
	}

	public void addBasedOn(LocationChangedEvent event) {
		basedOn.add(event);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
