package svend.storm.example.conference;

import storm.trident.TridentTopology;
import svend.storm.example.conference.input.EventBuilder;
import svend.storm.example.conference.input.ExtractCorrelationId;
import svend.storm.example.conference.input.SimpleFileStringSpout;
import svend.storm.example.conference.period.PeriodBackingMap;
import svend.storm.example.conference.period.PeriodBuilder;
import svend.storm.example.conference.timeline.BuildHourlyUpdateInfo;
import svend.storm.example.conference.timeline.IsPeriodComplete;
import svend.storm.example.conference.timeline.TimelineBackingMap;
import svend.storm.example.conference.timeline.TimelineUpdater;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


/**
 * definition + execution of the Trident topology
 *
 */
public class RunMe {

	public static void main(String[] args)  {
		
		// wipes out DB content at every start-up
		CassandraDB.DB.reset();
		
		TridentTopology topology = new TridentTopology();

		topology
			// reading events
			.newStream("occupancy", new SimpleFileStringSpout("data/events.json", "rawOccupancyEvent"))
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
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("occupancyTopology", null, topology.build());
		
		// this is soooo elegant...
		Utils.sleep(60000);
		cluster.killTopology("occupancyTopology");
		
		CassandraDB.DB.close();
	}
	
}
