package svend.storm.example.conference;

import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import svend.storm.example.conference.input.*;
import svend.storm.example.conference.period.PeriodBackingMap;
import svend.storm.example.conference.period.PeriodBuilder;
import svend.storm.example.conference.timeline.BuildHourlyUpdateInfo;
import svend.storm.example.conference.timeline.IsPeriodComplete;
import svend.storm.example.conference.timeline.TimelineBackingMap;
import svend.storm.example.conference.timeline.TimelineUpdater;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class BuildTopology {

	public static StormTopology build(TridentKafkaConfig kafkaConf) {
		
		TridentTopology topology = new TridentTopology();

		topology
			// reading and parsing events
			.newStream("occupancy", new OpaqueTridentKafkaSpout(kafkaConf))
            .each(new Fields("bytes"), new BytesToString(), new Fields("rawOccupancyEvent"))
            .each(new Fields("rawOccupancyEvent"), new EventBuilder(), new Fields("occupancyEvent"))

            // gathering "enter" and "leave" events into "presence periods"
            .each(new Fields("occupancyEvent"), new ExtractCorrelationId(), new Fields("correlationId"))
            .groupBy(new Fields("correlationId"))
            .persistentAggregate(PeriodBackingMap.FACTORY, new Fields("occupancyEvent"), new PeriodBuilder(), new Fields("presencePeriod"))
            .newValuesStream()

            // building room timelines as a state maintained in Cassandra
            .each(new Fields("presencePeriod"), new IsPeriodComplete())
            .each(new Fields("presencePeriod"), new BuildHourlyUpdateInfo(), new Fields("roomId", "roundStartTime"))
            .groupBy(new Fields("roomId", "roundStartTime"))
            .persistentAggregate(TimelineBackingMap.FACTORY, new Fields("presencePeriod", "roomId", "roundStartTime"), new TimelineUpdater(), new Fields("hourlyTimeline"))
			;
		
		return topology.build();

	}
	
	
}
