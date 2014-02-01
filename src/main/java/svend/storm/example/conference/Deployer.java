package svend.storm.example.conference;

import storm.kafka.KafkaConfig;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.spout.RichSpoutBatchExecutor;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

import java.util.Collections;


/**
 * definition + execution of the Trident topology
 *
 * usage:
 *
 * storm jar target/stormRoomOccupancy-0.0.2-SNAPSHOT-jar-with-dependencies.jar svend.storm.example.conference.Deployer
 *
 */
public class Deployer {
	

    // my whole setup is in a single local vagrant box
	public static String ENV_IP = "192.168.33.10";
	public static String CASSANDRA_IP = ENV_IP;
	public static String NIMBUS_IP = ENV_IP;
	public static String KAFKA_BROKER_IP = ENV_IP;

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException  {

        // before deploying anything, let's reset the data in Cassandra
        CassandraDB DB = new CassandraDB(CASSANDRA_IP);
        DB.reset();

		
		Config stormConfig = new Config();
		stormConfig.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 100);
		stormConfig.put("nimbus.host", NIMBUS_IP);
		stormConfig.put("svend.example.cassandra.ip", CASSANDRA_IP);          // read in the CassandraDB

        TridentKafkaConfig kafkaConf = new TridentKafkaConfig(
                KafkaConfig.StaticHosts.fromHostString(Collections.singletonList(KAFKA_BROKER_IP), 1) ,
                "room_events" );
        kafkaConf.forceFromStart = true;
        kafkaConf.fetchSizeBytes = 10000;

        StormSubmitter.submitTopology("occupancyTopology", stormConfig, BuildTopology.build(kafkaConf));
	}

}
