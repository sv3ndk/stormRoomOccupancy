stormRoomOccupancy
==================

Example of basic Storm topology that updates DB persistent state. based on Storm 0.8.2, Cassandra 1.2.5 and Java 7.

In order to run this example, an instance of Cassandra with the following key space is required: 

```
CREATE KEYSPACE EVENT_POC WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' } ;
```