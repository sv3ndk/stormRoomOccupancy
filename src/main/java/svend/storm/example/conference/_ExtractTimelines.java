package svend.storm.example.conference;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import svend.storm.example.conference.timeline.HourlyTimeline;

/**
 * short "java script" (aha) to provide an easy to import csv file from R (after we have processed plenty of tuples in the topology and want to look at them) 
 *
 */
public class _ExtractTimelines {
	
	public static void main(String[] args) 
	
	{
		
		System.out.println("dumping timelines from Cassandra to data/timelines.csv...");
		
		CassandraDB DB = new CassandraDB(Deployer.ENV_IP);
		
		try (FileWriter fw = new FileWriter(new File("data/timelines.csv"))) {
			for (HourlyTimeline timeline : DB.getAllTimelines()) {
				fw.write(timeline.toCsv() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("...done");
		
	}

}
