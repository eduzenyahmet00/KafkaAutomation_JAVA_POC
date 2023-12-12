package com.visa.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SendMessageToTopic {

   public static void main(String[] args) {

      for (int i = 1; i <=5; i++) { //to send message many times


         //String brokers = "glider-01.srvs.cloudkafka.com:9094,glider-02.srvs.cloudkafka.com:9094"; //Multiple Servers
         String brokers = "dory.srvs.cloudkafka.com:9094";
         String topic = "shetjzgv-ahmet";

         String username = "shetjzgv";
         String password = "ttGUKSaFvZ9iHA503YvL4OaWF5-4GSLi";


         Properties properties = new Properties();
         properties.setProperty("bootstrap.servers", brokers);

         //SERIALIZER: converts objects to bytes
         properties.setProperty("key.serializer", StringSerializer.class.getName());
         properties.setProperty("value.serializer", StringSerializer.class.getName());

         //Handle authentication
         properties.setProperty("security.protocol", "SASL_SSL");
         properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
         properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";");


         //this block is optional
         properties.put("enable.auto.commit", "true");
         properties.put("auto.commit.interval.ms", "100");
         properties.put("auto.offset.reset", "earliest");
         properties.put("session.timeout.ms", "30000");


         //Create kafka producer object
         KafkaProducer<String, String> myproducer = new KafkaProducer<>(properties);

         //ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic2, "key", "value");
         //CASE-1
         //ProducerRecord<String, String> message = new ProducerRecord<String, String>(topic, "Message-" + i, "Hey Neva-Eureka QA Team");
         ProducerRecord<String, String> message = new ProducerRecord<>(topic, "austin", "Hey Austin Team");
         //Randomly partition is being selected.

         //CASE-2
         //Send message to particular partition.
         //ProducerRecord<String,String> message = new ProducerRecord<>(topic, 2,"part2","Puplish Message to partition2");

         //ProducerRecord<String,String> message = new ProducerRecord<String, String>(topic2, 3,"part3","Puplish Message to partition3");

         //CASE-3
         //We can send the messages to different topics at the same time.


         myproducer.send(message);
         //myproducer.send(message1);
         //myproducer.send(message2);


         myproducer.flush();
         myproducer.close();
         System.out.println("Message puplished to topic: " + topic + " successfully!");

         //}


      }
   }
}