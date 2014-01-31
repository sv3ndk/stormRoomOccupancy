package svend.storm.example.conference.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * 
 * Quick and dirty spout which emits one tuple containing one single String value for each line found in a file. <br />
 * There is no support of fail() nor ack() here => failed tuples are just lost
 * 
 */
public class SimpleFileStringSpout extends BaseRichSpout {

	private final String emittedTupleName;

	private BufferedReader br;
	private SpoutOutputCollector collector;
	private String sourceFileName;

	public SimpleFileStringSpout(String sourceFileName, String emittedTupleName) {
		super();
		this.emittedTupleName = emittedTupleName;
		this.sourceFileName = sourceFileName;

	}

    @Override
    public void activate() {
    	System.out.println("activating");
    }

    @Override
    public void deactivate() {
    }

	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		System.out.println("opening");
		this.collector = collector;
		try {
			br = new BufferedReader(new FileReader(new File(sourceFileName)));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		if (br != null) {
			try {
				br.close();
				br = null;
			} catch (IOException e) {
				System.err.println("could not close input stream to json file containing events.");
				e.printStackTrace();
			}
		}
	}

	public void nextTuple() {
		System.out.println("simple text emitting ...");
		try {
			String rawEvent = br.readLine();
			if (rawEvent != null) {

				try {
					String messageId = UUID.randomUUID().toString();
					System.out.println("this: " + rawEvent);
					collector.emit(new Values(rawEvent), messageId);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				Utils.sleep(100);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(emittedTupleName));
	}

	@Override
	public void ack(Object msgId) {
		// yeah, look mommy, it worked!
	}

	@Override
	public void fail(Object msgId) {
		System.err.println("oups: failed message: " + msgId + ", not retrying");
	}

}
