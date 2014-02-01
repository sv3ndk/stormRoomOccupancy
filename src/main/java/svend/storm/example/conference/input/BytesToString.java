package svend.storm.example.conference.input;


import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.UnsupportedEncodingException;

/**
 */
public class BytesToString extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
        try {
            String asString = new String((byte[]) tuple.getValueByField("bytes"), "UTF-8");
            tridentCollector.emit(new Values(asString));
        } catch (UnsupportedEncodingException e) {
            System.err.println("ERROR: lost data: unable to parse inbound message from Kafka (expecting UTF-8 string)");
            e.printStackTrace();
        }
    }
}
