stormRoomOccupancy
==================

Basic Storm topology that updates DB persistent state. based on Storm 0.8.2, Cassandra 1.2.5 and Java 7.

This example is explained in great details here: 
http://svendvanderveken.wordpress.com/2013/07/30/scalable-real-time-state-update-with-storm/

In order to run this example, an instance of Cassandra with the following key space is required: 

```
CREATE KEYSPACE EVENT_POC WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' } ;
```