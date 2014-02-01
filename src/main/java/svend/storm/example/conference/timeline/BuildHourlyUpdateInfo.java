package svend.storm.example.conference.timeline;

import java.util.LinkedList;
import java.util.List;

import org.joda.time.DateTime;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import svend.storm.example.conference.period.RoomPresencePeriod;
import backtype.storm.tuple.Values;

/**
 * Emits one tuple per started hour overlapping with the {@link RoomPresencePeriod} of the input tuple. 
 * 
 */
public class BuildHourlyUpdateInfo extends BaseFunction {

	public static final int ONE_HOUR_MILLIS = 60 * 60 * 1000;

	public void execute(TridentTuple tuple, TridentCollector collector) {
		
		RoomPresencePeriod periodEvent = (RoomPresencePeriod) tuple.getValueByField("presencePeriod");
		for (Long roundStartTime : generateRoundPeriods(periodEvent.getStartTime(), periodEvent.getEndTme())) {
			collector.emit(new Values(periodEvent.getRoomId(), roundStartTime));
		}
	}
	
	/**
	 * creates the list of "round hours" 
	 */
	private List<Long> generateRoundPeriods(Long startTime, Long endTime) {
		List<Long> roundPeriods = new LinkedList<>();
		for (Long roundStartTime = ceilHourTime(startTime); roundStartTime < ceilHourTime(endTime) + ONE_HOUR_MILLIS; roundStartTime += ONE_HOUR_MILLIS) {
			roundPeriods.add(roundStartTime);
		}
		return roundPeriods;
	}

	/**
	 * @return a millis timestamp corresponding to dateMillis rounded to the previous hour"
	 */
	private Long ceilHourTime(Long dateMillis) {
		DateTime exact = new DateTime(dateMillis);
		return new DateTime(exact.getYear(), exact.getMonthOfYear(), exact.getDayOfMonth(), exact.getHourOfDay(), 0).getMillis();
	}
	
}