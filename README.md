stormRoomOccupancy
==================

Basic Storm topology that updates DB persistent state. based on Storm 0.8.2, Cassandra 1.2.5 and Java 7.

This example is explained in great details here: 
http://svendvanderveken.wordpress.com/2013/07/30/scalable-real-time-state-update-with-storm/

In order to run this example, an instance of Cassandra with the following key space is required: 

```
CREATE KEYSPACE EVENT_POC WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' } ;
```

(tables are re-created everytime time the topology is re-deployed)

Maybe edit this line in Deployer.java if you Nimbus is not reachable on that IP:

```
config.put("nimbus.host" , "192.168.33.10");
```

Then package the topology:

```
mvn package
```

And deploy it: 

```
storm jar target/stormRoomOccupancy-0.0.2-SNAPSHOT-jar-with-dependencies.jar svend.storm.example.conference.Deployer
```


