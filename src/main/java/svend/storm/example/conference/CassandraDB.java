package svend.storm.example.conference;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import svend.storm.example.conference.period.RoomPresencePeriod;
import svend.storm.example.conference.timeline.HourlyTimeline;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class CassandraDB {

	// quick and dirty JVM-wide singleton

	private final Cluster cluster;
	private Session session;

	private ObjectMapper mapper = new ObjectMapper();

	public CassandraDB(Map stormConfig) {
		this((String) stormConfig.get("svend.example.cassandra.ip"));
	}
	
	public CassandraDB(String cassandraIP) {
		System.out.println("build cassandra DB");
		cluster = Cluster.builder().addContactPoint(cassandraIP).build();
		reset();

		// don't forget to create that keyspace in Cassandra before try to run this, this command should do:
		// CREATE KEYSPACE EVENT_POC WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' } ;
		// ( the table creation will be automatic)
		System.out.println("cassandra DB built");
	}
	
	//////////////
	// room presence periods
	//////////////
	
	public List<RoomPresencePeriod> getPresencePeriods(List<String> correlationIds) {

		// using just the correlation provided in the event as Cassandra id (brr....) 
		ResultSet rs = execute("select id, payload from presence where id in ( " + toCsv(correlationIds) + " ) ");

		Map<String, RoomPresencePeriod> fromDB = unmarshallResultSet(rs, RoomPresencePeriod.class, "payload");

		// this ensures we return a list of the same size as the one received as input (may contain null values)
		List<RoomPresencePeriod> result = new ArrayList<RoomPresencePeriod>(correlationIds.size());
		for (String corrId : correlationIds) {
			result.add(fromDB.get(corrId));
		}

		return result;
	}

	public void upsertPeriods(List<RoomPresencePeriod> periods) {
		for (RoomPresencePeriod rpp : periods) {
			try {
				String periodJson = mapper.writeValueAsString(rpp);
				PreparedStatement statement = getSession().prepare("INSERT INTO presence  (id, payload) values (?,?)");
				execute(new BoundStatement(statement).bind(rpp.getId(), periodJson));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	//////////////
	// timelines
	//////////////


	public List<HourlyTimeline> getTimelines(List<Pair<String, Long>> roomIdAndStartTime) {

		List<String> ids = new ArrayList<>(roomIdAndStartTime.size());
		for (Pair<String, Long> roomidAndStartTime : roomIdAndStartTime) {
			ids.add(buildTimelineId(roomidAndStartTime.first, roomidAndStartTime.second));
		}

		ResultSet rs = execute("select id, timeline from room_timelines where id in ( " + toCsv(ids) + " ) ");
		Map<String, HourlyTimeline> fromDB = unmarshallResultSet(rs, HourlyTimeline.class, "timeline");

		// this ensures we return a list of the same size as the one received as input (may contain null values)
		List<HourlyTimeline> result = new ArrayList<>(roomIdAndStartTime.size());
		for (String id : ids) {
			result.add(fromDB.get(id));
		}

		return result;
	}
	
	
	public Collection<HourlyTimeline> getAllTimelines() {
		ResultSet rs = execute("select id, timeline from room_timelines ");
		return unmarshallResultSet(rs, HourlyTimeline.class, "timeline").values();
	}

	

	
	public void upsertTimelines(List<HourlyTimeline> timelines) {
		for (HourlyTimeline timeline : timelines) {
			try {
				String id = buildTimelineId(timeline.getRoomId(), timeline.getSliceStartMillis());
				String timelineJson = mapper.writeValueAsString(timeline);
				PreparedStatement statement = getSession().prepare("INSERT INTO room_timelines  (id, timeline) values (?,?)");
				execute(new BoundStatement(statement).bind(id, timelineJson));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	
	private String buildTimelineId(String roomId, Long startTime) {
		return roomId + "_" + (long) Math.ceil(startTime);
	}

	//////////////
	// generic DB stuff
	//////////////


	public void reset() {
		recreateTable("presence", "(id text PRIMARY KEY, payload TEXT)");
		recreateTable("room_timelines", "(id text PRIMARY KEY, timeline TEXT)");
	}

	private void recreateTable(String tableName, String spec) {
		try {
			execute("drop table " + tableName);
		} catch (InvalidQueryException ex) {
			System.err.println("warning: could not drop table " + tableName + ", is the code executed for the first time?");
		}
		execute("create table  " + tableName + " " + spec);
	}

	protected ResultSet execute(Query query) {
		if (query != null) {
			return getSession().execute(query);
		}
		return null;
	}

	protected ResultSet execute(String query) {
		if (query != null) {
			return getSession().execute(query);
		}
		return null;
	}

	// /////////////////////////////////
	// some utility methods

	private String toCsv(List<String> vals) {
		StringBuffer stb = new StringBuffer();
		boolean first = true;
		for (String val : vals) {
			if (!first) {
				stb.append(" , ");
			}
			stb.append(" '").append(val).append("' ");
			first = false;
		}
		return stb.toString();
	}

	/**
	 * expects this resultSet to contain a "id" and a json field called fieldName => unmarshalls that and return a map
	 */
	private <T> Map<String, T> unmarshallResultSet(ResultSet resultSet, Class<T> expectedClass, String fieldName) {

		Map<String, T> fromDB = new HashMap<String, T>();
		while (!resultSet.isExhausted()) {
			try {
				Row row = resultSet.one();
				T unmarshalled = mapper.readValue(row.getString(fieldName), expectedClass);
				fromDB.put(row.getString("id"), unmarshalled);
			} catch (Exception e) {

				e.printStackTrace();
			}
		}
		return fromDB;
	}
	
	protected Session getSession() {
		if (session == null) {
			synchronized (this) {
				if (session == null) {
					session = cluster.connect("EVENT_POC");
				}
			}
		}
		return session;

	}


}