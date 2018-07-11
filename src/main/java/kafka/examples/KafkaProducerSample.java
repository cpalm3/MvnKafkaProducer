package kafka.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class KafkaProducerSample {
	
	 static final MetricRegistry metrics = new MetricRegistry();
	    Logger logger = Logger.getLogger(KafkaProducerSample.class.getName());
	    
	static void startConsoleReport() {
	    ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
	        .convertRatesTo(TimeUnit.SECONDS)
	        .convertDurationsTo(TimeUnit.MILLISECONDS)
	        .build();
	    reporter.start(1, TimeUnit.SECONDS);
	}
	
	static void startLogReport(Integer pollingRate) {
		Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
                .outputTo(LoggerFactory.getLogger("com.example.metrics"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
		reporter.start(pollingRate, TimeUnit.SECONDS);
	}
	

	
	public void runSample(String topic, String bootStrapServers,  Integer batchSize,Integer numIterations,
			Integer recordSize, Integer buffMemory,Boolean isAsync, Integer maxBlockMS){
		logger.info("runSample "+numIterations);
		Properties props = new Properties();
		 props.put("bootstrap.servers", bootStrapServers);
		 props.put("acks", "all"); 
		 props.put("retries", 100);
		 props.put("retry.backoff.ms",100);
		 
		 props.put("batch.size", batchSize);//Need to see if this number should increase under load
		// props.put("batch.size", 16384);
		 props.put("linger.ms", 1);//After 1 ms fire the batch even if the batch isn't full.
		 props.put("buffer.memory", buffMemory);//33554432  or 18000 to hit buffer full and block
		 props.put("request.timeout.ms",60000);//Default is 30 sec
		// props.put("block.on.buffer.full", blockOnBufferFull);
		 
		// if(!blockOnBufferFull){
			 props.put("max.block.ms",maxBlockMS);//60000 default value
		// }
		//props.put("producer.type", "async");
		 props.put("max.in.flight.requests.per.connection", 1);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 //props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		 
		
		 Producer<String, String> producer = new KafkaProducer(props); 
		//Producer<String, byte[]> producer = new KafkaProducer(props); 

		// Meter requests = metrics.meter("requests");
		 Timer requests = metrics.timer("requests");
		/* Used for binary payloads, need to change serializer type above to use
		 byte[] bytes = new byte[recordSize];
	        Arrays.fill( bytes, (byte) 'a' );   */
	      
		 //Used with string Serializer type
		  StringBuffer sb = new StringBuffer();
		  Integer payLen = new Integer(0);
		  while(payLen < (recordSize)){ //Convert to 16 bit comparison
		  sb.append("SensorData "+ new Date().toString() +" "+System.currentTimeMillis());
		   payLen = (sb.length()*2);//number of 16 bit  divide by 2, is number of 8 bits ~4000 is 8k
		  }

         Statistics stats = new Statistics(numIterations);
         
         if(isAsync){
        	// System.out.println("TRUE");
        	 logger.info("ASycn producer");
			 for(int keyId = 0; keyId < numIterations; keyId++){			
				 //requests.mark(); 
				 long start = System.currentTimeMillis();
					Timer.Context ct;
					ct = requests.time();
					  
					try{
					// producer.send(new ProducerRecord<String, String>(topic, Integer.toString(tileid), sb.toString()+System.currentTimeMillis()));
					//producer.send(new ProducerRecord<String, byte[]>(topic, Integer.toString(tileid), bytes));
						//ProducerRecord<String, byte[]> record = new  ProducerRecord<String, byte[]>(topic, Integer.toString(tileid), bytes);
						ProducerRecord<String, String> record = new  ProducerRecord<String, String>(topic, Integer.toString(keyId), sb.toString()+System.currentTimeMillis());
					//producer.send(new ProducerRecord<String, byte[]>(topic, Integer.toString(tileid), bytes)).get();
					producer.send(record,
								new Callback() {
		                     public void onCompletion(RecordMetadata metadata, Exception e) {
		                         if(e != null){
		                        	logger.error("Failed Record is "+record.key()+" "+e);
		                        	System.out.println("Failed Record is "+record.key()+" "+e);
		                        	// e.printStackTrace();
		                            //TODO close producer if failed to send after retries.
		                        	//producer.close(0,TimeUnit.MILLISECONDS);

		                         }
		                         if(metadata != null)
		                        	 logger.debug("Record Key "+ record.key()+"Metadata is "+metadata.offset() +" "+ metadata.partition() +" "+ metadata.topic());
		                            // System.out.println("Record Key "+ record.key()+" The offset of the record we just sent is: " + metadata.offset());
		                         //TODO Perform any Successfull Actions HERE, If consuming from a previous Queue, would ack here.
		                     
		                     } });
						
					  
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}finally{
					 ct.stop();
					 //producer.close();//Close to avoid resource leak
					 
					
				 }
					long end = System.currentTimeMillis();
					 stats.add(new Metrics(end-start, recordSize*batchSize, 0, batchSize));
				 
			 }
         }else{
        	 //System.out.println("FALSE");
        	 logger.info("Sycn producer");
        	 for(int tileid = 0; tileid < numIterations; tileid++){
				 //requests.mark(); 
				 long start = System.currentTimeMillis();
					Timer.Context ct;
					ct = requests.time();
					  
					try{
					
					producer.send(new ProducerRecord<String, String>(topic, Integer.toString(tileid), sb.toString())).get();
						
						
					  
					} catch (Exception e) {
						System.out.println("Error on TileID "+tileid);
						// TODO Auto-generated catch block
						e.printStackTrace();
					}finally{
					 ct.stop();
					
				 }
					long end = System.currentTimeMillis();
					 stats.add(new Metrics(end-start, recordSize*batchSize, 0, batchSize));
				 
			 }
        	 
         }
		   /* This is for testing out the linger time fires, need to keep jvm alive longer than linger time
		  *  try {
			Thread.sleep(2);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} */
		 stats.show();
		 logger.info("Closing Producer");
		 
		   producer.close();
	}

	public static void main(String[] args) {
		KafkaProducerSample kps = new KafkaProducerSample();
		if(args.length < 8){
			System.out.println("Usage: Topic, Server, batchSize, numIterations, metricsPollingRateInSeconds,"
					+ " recordSize in bytes, buffMem, isAsync, maxblockms ");
			System.exit(0);
		}
		String topic = args[0];
		String server = args[1];
		String batchSize = args[2]; 
		String numIterations = args[3];
		String pollingRate = args[4];
		String recordSize = args[5];
	    String buffMem = args[6];
	    String async = args[7];
	    
		startLogReport(new Integer(pollingRate));
		//startConsoleReport();
	    
		kps.runSample(topic,server,	new Integer(batchSize), new Integer(numIterations), new Integer(recordSize),
				new Integer(buffMem), new Boolean(async),  new Integer(args[8]) );

	}

}
