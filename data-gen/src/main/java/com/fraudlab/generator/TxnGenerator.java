package com.fraudlab.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class TxnGenerator {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        List<String> flags = Arrays.asList(args);
        boolean cardTesting = flags.contains("--card-testing");
        boolean impossibleTravel = flags.contains("--impossible-travel");

        Config conf = ConfigFactory.load();
        String brokers = System.getenv().getOrDefault("BROKER", conf.getString("kafka.brokers"));
        String topic = System.getenv().getOrDefault("TOPIC", "txns");

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);

        Random rnd = new Random(1);
        long txnId = 0;
        long userId = 1;
        double lat = 40.0, lon = -73.0; // NYC
        while(true) {
            if (impossibleTravel && txnId % 2 == 1) {
                // jump to Europe
                lat = 48.0; lon = 2.0; // Paris
            } else {
                lat = 40.0; lon = -73.0;
            }
            String card = cardTesting ? "card-" + (txnId % 2) : "card-"+txnId;
            Map<String,Object> txn = new LinkedHashMap<>();
            txn.put("txn_id", "t"+txnId);
            txn.put("event_time", System.currentTimeMillis());
            txn.put("card_id", card);
            txn.put("user_id", "u"+userId);
            txn.put("merchant_id", "m1");
            txn.put("mcc", "5411");
            txn.put("amount", cardTesting ? 1.0 : 50.0 + rnd.nextInt(50));
            txn.put("currency", "USD");
            txn.put("ip", "127.0.0.1");
            txn.put("device_id", "d1");
            txn.put("lat", lat);
            txn.put("lon", lon);
            txn.put("country", "US");
            txn.put("status", "APPROVED");
            txn.put("channel", "ecom");

            String json = MAPPER.writeValueAsString(txn);
            producer.send(new ProducerRecord<>(topic, (String)txn.get("txn_id"), json));
            txnId++;
            Thread.sleep(200);
        }
    }
}
