package svend.storm.example.conference;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class BF extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		System.out.println("gggggggggggggggggggggggggg");
		collector.emit(new Values("ohoho"));

	}

}
