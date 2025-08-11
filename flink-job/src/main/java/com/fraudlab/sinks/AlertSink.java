package com.fraudlab.sinks;

import com.fraudlab.model.Alert;
import com.fraudlab.serde.JsonSerde;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class AlertSink extends RichSinkFunction<Alert> {
    private final String brokers;
    private final String topic;
    private transient KafkaProducer<String, byte[]> producer;

    public AlertSink(String brokers, String topic) {
        this.brokers = brokers;
        this.topic = topic;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void invoke(Alert value, Context context) {
        try {
            byte[] b = new JsonSerde<>(Alert.class).serialize(value);
            producer.send(new ProducerRecord<>(topic, value.txnId, b));
            System.out.println("ALERT: " + value.detector + " " + value.message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}
