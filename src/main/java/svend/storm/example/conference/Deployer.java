package svend.storm.example.conference;

import storm.trident.spout.RichSpoutBatchExecutor;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;


/**
 * definition + execution of the Trident topology
 *
 */
public class Deployer {
	
	
	public static String ENV_IP = "192.168.33.10";
	

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException  {
		
		Config config = new Config();
		config.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 100);
		config.put("nimbus.host" , ENV_IP);
		config.put("svend.example.cassandra.ip" , ENV_IP);
		
		StormSubmitter.submitTopology("occupancyTopology", config, BuildTopology.build("/vagrant/data/events.json"));
		
		//CassandraDB.DB.close();
	}
	
	
	
	
	
	
}
