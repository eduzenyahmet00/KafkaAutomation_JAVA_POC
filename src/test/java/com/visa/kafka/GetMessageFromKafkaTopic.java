package com.visa.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class GetMessageFromKafkaTopic {

    public static void main(String[] args) {

        //https://customer.cloudkarafka.com/instance

        String brokers = "dory.srvs.cloudkafka.com:9094";
        String topic = "shetjzgv-ahmet";

        String username = "shetjzgv";
        String password = "ttGUKSaFvZ9iHA503YvL4OaWF5-4GSLi";


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);


        //DESERIALIZER: converts bytes to objects
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        //add
        properties.setProperty("auto.offset.reset", "latest");

        //Handle authentication
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""+username+"\" password=\""+password+"\";");


        //Add Consumer group: Every consumer belongs to a group
        //We should give some groupid for that.
        //properties.setProperty("group.id", "shetjzgv-ahmet");
        properties.put("group.id", "shetjzgv-ahmet");


        //A
        //Create consumer object using KafkaConsumer class
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //B
        //from which topic you have to consume
        consumer.subscribe(Arrays.asList(topic)); // One Topic
        //consumer.subscribe(Arrays.asList(topic,topic1)); //2 Topic
        //consumer.subscribe(Collections.singletonList(topic));


        //we are continueously listening the topic.
        //If it finds any records, we'll return.
////
//		// Poll for messages
        boolean flag = true;
        while(flag) {
            ConsumerRecords<String,String> records = consumer.poll(100); // Return N number of records
            for(ConsumerRecord<String,String> each:records) {
                String msg = each.value();
                System.out.println("Latest message from topic :" + msg);
                long offSet = each.offset();
                System.out.println("Offset value :" + offSet);

                if (offSet>0) {
                    flag = false;
                    consumer.close();
                }


            }
        }

    }
}
