package com.example.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrder {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties()); // Instance a new Kafka producer with your key and message type. The producer instance waits for some properties.
        var value = "2312, 12312, 321"; // Static message.
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value); // Instance a new ProducerRecord and set one topic, key, and value. I'm lost on what's the key function.
        producer.send(record, (data, ex) -> { // Error handling.
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.printf("sucess send " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        }).get();
    }

    private static Properties properties() {
        var proprieties = new Properties();
        proprieties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // Set IP and PORT the Kafka server.
        proprieties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Serializer String for bytes.
        proprieties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Serializer String for bytes.
        return proprieties;
    }
}
