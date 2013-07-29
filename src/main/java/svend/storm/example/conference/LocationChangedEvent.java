package svend.storm.example.conference;


public class LocationChangedEvent {

	private String id;
	private String corrId;
	private String userId;
	private EVENT_TYPE eventType;
	private Long time;

	private String roomId;

	public String getId() {
		return id;
	}

	public void setId(String eventId) {
		this.id = eventId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public EVENT_TYPE getEventType() {
		return eventType;
	}

	public void setEventType(EVENT_TYPE eventType) {
		this.eventType = eventType;
	}

	public String getRoomId() {
		return roomId;
	}

	public void setRoomId(String roomId) {
		this.roomId = roomId;
	}

	public enum EVENT_TYPE {
		ENTER, LEAVE
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	// event identity is based only on its id
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LocationChangedEvent other = (LocationChangedEvent) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	public String getCorrId() {
		return corrId;
	}

	public void setCorrId(String corrId) {
		this.corrId = corrId;
	}

}
