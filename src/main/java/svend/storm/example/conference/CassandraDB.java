package svend.storm.example.conference;


import static java.lang.String.format;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.topology.FailedException;
import com.datastax.driver.core.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import storm.trident.state.OpaqueValue;
import svend.storm.example.conference.period.RoomPresencePeriod;

import com.datastax.driver.core.exceptions.InvalidQueryException;

public class CassandraDB {

	private final Cluster cluster;
	private Session session;

    private static Logger logger = LogManager.getLogger(CassandraDB.class.getName());

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

    ///////
    // generic opaque put/get (don't do this: get a real Cassandrabacking, e.g. the one from storm-contrib)




    public void put(String table, List<String> keys, List<OpaqueValue> opaqueStrings) {

        // this could be optimzed with batches..
        for (Pair<String, OpaqueValue> keyValue : Utils.zip(keys, opaqueStrings)) {
            PreparedStatement statement = getSession().prepare(format("INSERT INTO %s (id, txid, prev, curr) values (?,?)", table));
            execute(new BoundStatement(statement).bind(keyValue.getKey(), keyValue.getValue().getCurr(), keyValue.getValue().getPrev()));
        }
    }

    public List<OpaqueValue> get(String table, List<String> keys) {

        List<OpaqueValue> vals = new ArrayList<>(keys.size());
        ResultSet rs = execute(format("select id, txid, prev, curr from %s where id in ( %s ) ", table, toCsv(keys) ));
        Map<String, OpaqueValue> data = toMapOfOpaque(rs);
        for (String key: keys){
            vals.add(data.get(key));
        }

        return vals;
    }



    //////////////
	// room presence periods old-style DAO
	//////////////
	
	public List<RoomPresencePeriod> getPresencePeriods(List<String> correlationIds) {

		// using just the correlation provided in the event as Cassandra id (brr....) 
		ResultSet rs = execute("select id, payload from presence where id in ( " + toCsv(correlationIds) + " ) ");

		Map<String, RoomPresencePeriod> fromDB = unmarshallResultSet(rs, RoomPresencePeriod.class, "payload");

		// this ensures we return a list of the same size as the one received as input (may contain null values)
		List<RoomPresencePeriod> result = new ArrayList<>(correlationIds.size());
		for (String corrId : correlationIds) {
			result.add(fromDB.get(corrId));
		}

		return result;
	}

	public void upsertPeriods(List<RoomPresencePeriod> periods) {
		for (RoomPresencePeriod rpp : periods) {

            String periodJson;
			try {
				periodJson = mapper.writeValueAsString(rpp);
            } catch (IOException e) {
                // this should typically be recorded somewhere to a dead letter queue for some human to look at
                logger.error("impossible to serialize a room presence period object to JSON => THIS DATA IS LOST!", e);
                continue;
            }

            try {
				PreparedStatement statement = getSession().prepare("INSERT INTO presence (id, payload) values (?,?)");
				execute(new BoundStatement(statement).bind(rpp.getId(), periodJson));
			} catch (Exception e) {
                logger.error("error while contacting Cassandra, triggering a retry...", e);
				new FailedException("error while trying to record room presence in Cassandra ", e);
			}
		}
	}


	//////////////
	// generic DB stuff
	//////////////


	public void reset() {
		recreateTable("presence", "(id text PRIMARY KEY, payload TEXT)");
		recreateTable("room_timelines", "(id text PRIMARY KEY, txid bigint, curr TEXT, prev TEXT)");
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
	 * unbox the id, txid, prev, c
	 */
	private  Map<String, OpaqueValue> toMapOfOpaque(ResultSet resultSet) {

		Map<String, OpaqueValue> fromDB = new HashMap<>();
		while (!resultSet.isExhausted()) {
			try {
				Row row = resultSet.one();
                OpaqueValue opaqueVal = new OpaqueValue(row.getLong("txid"), row.getString("prev"), row.getString("curr"));
                fromDB.put(row.getString("id"), opaqueVal);
			} catch (Exception e) {
				System.err.println("failed to unmarshall data from DB => data is now lost!");
                e.printStackTrace();
			}
		}
		return fromDB;
	}


    /**
     * expects this resultSet to contain a "id" and a json field called fieldName => unmarshalls that and return a map
     */
    private <T> Map<String, T> unmarshallResultSet(ResultSet resultSet, Class<T> expectedClass, String fieldName) {

        Map<String, T> fromDB = new HashMap<>();
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