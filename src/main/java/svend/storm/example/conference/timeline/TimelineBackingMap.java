package svend.storm.example.conference.timeline;

import static java.lang.String.format;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.topology.FailedException;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.OpaqueMap;
import svend.storm.example.conference.CassandraDB;
import backtype.storm.task.IMetricsContext;
import svend.storm.example.conference.Utils;

public class TimelineBackingMap implements IBackingMap<OpaqueValue> {
	
	private final CassandraDB DB;
	
	public TimelineBackingMap(CassandraDB dB) {
		DB = dB;
	}

	@Override
	public List<OpaqueValue> multiGet(List<List<Object>> keys) {

        List<OpaqueValue> opaqueStrings;
        try {
            opaqueStrings = DB.get("room_timelines", toSingleKeys(keys));
        } catch (Exception e) {
            System.err.print("error while storing timelines to cassandra");
            e.printStackTrace();
            throw new FailedException("could not store data into Cassandra", e);
        }

        try {
            return Utils.opaqueStringToOpaqueValues(opaqueStrings, HourlyTimeline.class);
        } catch (IOException e) {
            System.err.println("error while trying to deserialize data from json => giving up (data is lost!)");
            return Utils.listOfNulls(keys.size());      // this assumes previous state does not exist => destroys data!
        }

    }

	@Override
	public void multiPut(List<List<Object>> keys, List<OpaqueValue> timelines) {

        List<OpaqueValue> jsonOpaqueTimelines;
        try {
            jsonOpaqueTimelines = Utils.opaqueValuesToOpaqueJson(timelines);
        } catch (IOException e) {
            System.err.println("error while trying to serialize data to json => giving up (data is lost!)");
            return;
        }

        if (jsonOpaqueTimelines != null) {
            try {
                DB.put("room_timelines", toSingleKeys(keys), jsonOpaqueTimelines);
            } catch (Exception e)  {
                System.err.print("error while storing timelines to cassandra, triggering a retry...");
                e.printStackTrace();
                throw new FailedException("could not store data into Cassandra, triggering a retry...", e);
            }
        }

	}

	public static StateFactory FACTORY = new StateFactory() {
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return OpaqueMap.build(new TimelineBackingMap(new CassandraDB(conf)));
		}
	};


    private List<String> toSingleKeys(List<List<Object>> keys) {
        List<String> result = new ArrayList<>(keys.size());
        for (List<Object> key: keys) {
            // the keys we get here are those specified in the groupBy => Roomid and startId
            result.add(format("%s_%d", key.get(0), (Long) key.get(1)));
        }
        return result;
    }

    public HourlyTimeline[] getAllTimelines() {
        // TODO
        return null;
    }
}
