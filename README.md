# Kafka Producer Example
This is a single threaded Kafka producer that will show the usage of best practice parameters for minimizing data loss. Also shows the usage of onCompletion when working with the default AsyncProducer

## Properties
* props.put("acks", "all");
	* Waits for in sync replicas to reply
* props.put("retries", 100); props.put(“retry.backoff.ms”,100);
	* Retries for 10S, then throws error in callback
	* Validate if your volume can handle retries for 10s and lower retries and backoff as needed.
* props.put("max.block.ms",maxBlockMS);
	* 60,0000 default, Lowering this to small will cause failures on reconnect during broker failover scenarios.
* props.put("max.in.flight.requests.per.connection", 1);
	* To prevent reordering: close producer in callback with close(0) on send failure
* props.put("linger.ms", 1);
	* After 1 ms fire the batch even if the batch isn't full.
* props.put("buffer.memory", buffMemory);
	* Usecase and SLA dependent, must be larger than record size
* props.put("batch.size", batchSize);
	* Batch size with a linger setting allows better management of the channel under heavy load.




## Broker settings
* To prevent data loss on leader re-election for a 3 node Brokerage During Topic Creation
	* default.replication.factor=3
	* min.insync.replica=2
	* unclean.leader.election.enable = false
	* Replication factor > min.isr
		*  If replication factor = min.isr, partition will be offline when one replica is down
	*  Allows 2 out of the 3 replica’s to be in sync and Ack’d


### Example
Alter Topic level setting

bin/kafka-configs.sh --alter --zookeeper ZKADDRESS --
entity-type topics --entity-name TOPICNAME --add-config
min.insync.replicas=2 unclean.leader.election.enable=false

## Running
Usage: Topic, Server, batchSize, numIterations, metricsPollingRateInSeconds, recordSize in bytes, buffMem, isAsync, maxblockms, isTransational

java -jar target/KafkaProducerSample-0.0.1-SNAPSHOT.jar TOPIC BROKER:PORT,BROKER:PORT 10 3000 3 1000 10000 TRUE 60000 TRUE
