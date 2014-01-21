package svend.storm.example.conference.timeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;
import svend.storm.example.conference.CassandraDB;
import svend.storm.example.conference.Pair;
import backtype.storm.task.IMetricsContext;

public class TimelineBackingMap implements IBackingMap<HourlyTimeline> {
	
	private final CassandraDB DB;
	
	public TimelineBackingMap(CassandraDB dB) {
		super();
		DB = dB;
	}

	@Override
	public List<HourlyTimeline> multiGet(List<List<Object>> keys) {
		return DB.getTimelines(toTimelineQueryKeys(keys));
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<HourlyTimeline> timelines) {
		DB.upsertTimelines(timelines);
	}

	private List<Pair<String, Long>> toTimelineQueryKeys(List<List<Object>> keys) {
		List<Pair<String, Long>> result = new ArrayList<>(keys.size());
		for (List<Object> key: keys) {
			// the keys we get here are those specified in the groupBy => Roomid and startId
			result.add(new Pair<String, Long>((String) key.get(0), (Long) key.get(1)));
		}
		return result;
	}
	
	
	public static StateFactory FACTORY = new StateFactory() {
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			// TODO: replace this with OpaqueMap (but our spout never replays anything anyway... ^__^)
			System.out.println("making timeline state");
			return NonTransactionalMap.build(new TimelineBackingMap(new CassandraDB(conf)));
		}
	};

}
