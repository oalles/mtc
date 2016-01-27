# Mongo Tailable Consumer

###DESCRIPTION
MTC is task to be configured in order to consume documents from a tailable collection in a mongo database. It optionally allows to persist the id the last processed document in the collection so the task can be safely restarted.
 
Original idea from the  [camel-mongodb component](http://camel.apache.org/mongodb.html), totally rewritten and based on mongo-java-driver 3.  

###Configuration
1. A `DocumentHandler` implementation to manage each document being consumed from the database.
2. A MTCConfiguration object with:
	- A [MongoClient](http://api.mongodb.org/java/3.0/com/mongodb/MongoClient.html) instance to provide connection to MongoDb . 
	- A database and a collection name. 
	- Optionally, set with an MTCPersistentTrackingConfiguration instance. 

###Sample of Usage:
see [Simple Message Broadcaster](https://github.com/oalles/smb) 
 
[Omar Alles](https://omarall.es)