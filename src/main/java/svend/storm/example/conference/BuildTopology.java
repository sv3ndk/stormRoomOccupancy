package svend.storm.example.conference;

import storm.trident.TridentTopology;
import svend.storm.example.conference.input.CrappyTransactionalTextFileSpout;
import svend.storm.example.conference.input.EventBuilder;
import svend.storm.example.conference.input.ExtractCorrelationId;
import svend.storm.example.conference.input.SimpleFileStringSpout;
import svend.storm.example.conference.period.PeriodBackingMap;
import svend.storm.example.conference.period.PeriodBuilder;
import svend.storm.example.conference.timeline.BuildHourlyUpdateInfo;
import svend.storm.example.conference.timeline.IsPeriodComplete;
import svend.storm.example.conference.timeline.TimelineBackingMap;
import svend.storm.example.conference.timeline.TimelineUpdater;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.tuple.Fields;

public class BuildTopology {

	public static StormTopology build(String sourceFile) {
		
		TridentTopology topology = new TridentTopology();

		topology
			// reading events
//			.newStream("occupancy", new SimpleFileStringSpout(sourceFile, "rawOccupancyEvent"))
			.newStream("occupancy", new CrappyTransactionalTextFileSpout("rawOccupancyEvent", sourceFile, "UTF-8"))
//			.newStream("occupancy", new TestWordSpout())
			
//			.each(new Fields("word"), new BF(), new Fields("rawOccupancyEvent"))
			
			.each(new Fields("rawOccupancyEvent"), new EventBuilder(), new Fields("occupancyEvent"))
			
			// gathering "enter" and "leave" events into "presence periods"
			.each(new Fields("occupancyEvent"), new ExtractCorrelationId(), new Fields("correlationId"))
			.groupBy(new Fields("correlationId"))
			.persistentAggregate( PeriodBackingMap.FACTORY, new Fields("occupancyEvent"), new PeriodBuilder(), new Fields("presencePeriod"))
			.newValuesStream()
			
			// building room timeline 
			.each(new Fields("presencePeriod"), new IsPeriodComplete())
			.each(new Fields("presencePeriod"), new BuildHourlyUpdateInfo(), new Fields("roomId", "roundStartTime"))
			.groupBy(new Fields("roomId", "roundStartTime"))
			.persistentAggregate( TimelineBackingMap.FACTORY, new Fields("presencePeriod","roomId", "roundStartTime"), new TimelineUpdater(), new Fields("hourlyTimeline"))
			;
		
		return topology.build();

	}
	
	
}
