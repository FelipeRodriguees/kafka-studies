package com.example.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FraudDetectorService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties()); // Instance a new Kafka consumer with your key and message type. The producer instance waits for some properties.
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER")); // Define the topic for consumers. Is not common to listen to many topics.
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100)); // Verifica se o consumer achou alguma mensagem.
            if (!records.isEmpty()) {
                System.out.println("Found " + records.count() + " messages.");
                for (var record : records) {
                    System.out.println("*-----------------------------------------------*");
                    System.out.println("Processing new order, checking for froud!");
                    System.out.println("Key: " + record.key());
                    System.out.println("Value: " + record.value());
                    System.out.println("Partition: " + record.partition());
                    System.out.println("Offset: " + record.offset());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        // ignoring.
                        e.printStackTrace();
                    }
                    System.out.println("*-----------------------------------------------*");
                    System.out.println("Order processed!");
                    System.out.println("*-----------------------------------------------*");
                }
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // Set IP and PORT the Kafka server for consumption.
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Deserializer key value from byte to String.
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Deserializer value from byte to String.
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName()); // Define group names for consuming messages. All consumers using this id receive any messages, but all is consumed.
        return properties;
    }
}
