package svend.storm.example.conference.input;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import svend.storm.example.conference.LocationChangedEvent;
import backtype.storm.tuple.Values;

public class ExtractCorrelationId extends BaseFunction {
	public void execute(TridentTuple tuple, TridentCollector collector) {
		LocationChangedEvent event = (LocationChangedEvent) tuple.getValueByField("occupancyEvent");
		collector.emit(new Values(event.getCorrId()));
	}
}
