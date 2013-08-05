package svend.storm.example.conference.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * 
 * Quick and dirty "transactionnal" text file spout: emits one tuple with one single field for each line in the file.
    
 * 
 * Watch out that: 
 * 	<ul>
 * 		<li>It might consume a lot of memory: full payload of the emitted messages is kept in memory until the message are successfully acked</li>
 * 		<li>It's not restart proof: all state in maintained in memory as 2 static hashmap => if the worker should restart, it would just replays the file from memory. One would need
 * to keep the emittedMessages and txidMsgIds state persisted somewhere to fix that...</li>
 * 		<li>It's not very robust: failure while reading just bubble up to the framework (which *might* be ok if we were restart-proof...)</li>
 * 		<li>Reading a local file makes this spout, well, not partitioned! Look at stuff like HDFS spout for something more scalable</li>
 * </ul>
 * 
 * 	
 * 
 * 						This is a *toy*, use in prod only if you hate your job (and don't mention my name...) :)
 * 
 */
public class TransactionalTextFileSpout implements ITridentSpout<Set<String>> {

	private static final long serialVersionUID = 1L;

	private final String singleOutputFieldName;
	private final String encoding;
	private final String sourceFileName;

	// Set of messages ids emitted for each transaction id
	private final static Map<Long, Set<String>> txidMsgIds = new HashMap<>();

	// full String paylayd for each message id
	private final static Map<String, String> emittedMessages = new HashMap<>();

	public TransactionalTextFileSpout(String singleOutputFieldName, String sourceFileName, String encoding) {
		this.singleOutputFieldName = singleOutputFieldName;
		this.sourceFileName = sourceFileName;
		this.encoding = encoding;
	}


	// there's no synchronization one the access to the reader nor to the 2 State maps: Coordinator is executed by a single thread
	private class Coordinator implements BatchCoordinator<Set<String>> {

		private final long batchSize;

		private BufferedReader reader;

		public Coordinator(long batchSize) {
			this.batchSize = batchSize;
		}

		@Override
		public Set<String> initializeTransaction(long txid, Set<String> prevMetadata) {
			initIfNeeded();

			// the initialization is doing the actual read operation and keeping the result in memory, to be emitted by the emittor below

			try {
				Set<String> emittedIds = new HashSet<>();
				for (int idx = 0; idx < batchSize; idx++) {
					String rawLine = reader.readLine();
					if (rawLine != null) {
						try {
							String messageId = UUID.randomUUID().toString();
							emittedIds.add(messageId);
							emittedMessages.put(messageId, rawLine);
						} catch (Exception e) {
							// not very robust, heh?
							throw new RuntimeException("failed to read file " + sourceFileName, e);
						}
					} else {
						System.out.println("sleep");
						Utils.sleep(5);
					}
				}

				return emittedIds;

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}

		@Override
		public void success(long txid) {
			Set<String> emittedMsgIds = txidMsgIds.get(txid);
			if (emittedMsgIds != null) {
				for (String messageId : emittedMsgIds) {
					emittedMessages.remove(messageId);
				}
			}
			txidMsgIds.remove(txid);
		}

		@Override
		public boolean isReady(long txid) {
			return true;
		}

		@Override
		public void close() {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					// brrr....
					System.err.println("failed to close file from " + TransactionalTextFileSpout.class + " => giving up...");
					e.printStackTrace();
				}
			}
		}

		private void initIfNeeded() {
			if (reader == null) {
				try {
					reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(sourceFileName)), encoding));
				} catch (Exception e) {
					throw new RuntimeException("failed to initialize spout", e);
				}
			}
		}

	}

	private class TheEmitter implements Emitter<Set<String>> {

		@Override
		public void emitBatch(TransactionAttempt tx, Set<String> coordinatorMeta, TridentCollector collector) {

			// no NPTR check here: if null happen it's a bug => let's make the baby cry, we'll notice soon enough... 
			for (String messageId : txidMsgIds.get(tx.getTransactionId())) {
				assert emittedMessages.containsKey(messageId);
				String payload = emittedMessages.get(messageId);
				collector.emit(new Values(payload));
			}
		}

		@Override
		public void success(TransactionAttempt tx) {
			// NOP
		}

		@Override
		public void close() {
			// NOP
		}

	}

	@Override
	public BatchCoordinator<Set<String>> getCoordinator(String txStateId, Map conf, TopologyContext context) {
		return new Coordinator((Long) conf.get(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF));
	}

	@Override
	public Emitter<Set<String>> getEmitter(String txStateId, Map conf, TopologyContext context) {
		return new TheEmitter();
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(singleOutputFieldName);
	}

}
