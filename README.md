stormRoomOccupancy
==================

Basic Storm topology that updates DB persistent state. The code is based on Storm 0.9.0.1, Cassandra 2.0.4 and Java 7.

The [first release] ( https://github.com/svendx4f/stormRoomOccupancy/releases/tag/v1.0.1) is explained in great details in my blog on [scalable real-time state update with Storm] (http://svendvanderveken.wordpress.com/2013/07/30/scalable-real-time-state-update-with-storm/)

The current code contains an update explained in my blog post on [Storm error handling] ( http://svendvanderveken.wordpress.com/2014/02/01/notes-on-storm-trident-error-handling)

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


