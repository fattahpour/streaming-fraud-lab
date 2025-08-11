package com.fraudlab;

import com.fraudlab.cep.ImpossibleTravelCEP;
import com.fraudlab.ensemble.RiskBlenderFn;
import com.fraudlab.model.Alert;
import com.fraudlab.model.Transaction;
import com.fraudlab.rules.VelocityRuleFn;
import com.fraudlab.sinks.AlertSink;
import com.fraudlab.stats.ZScoreFn;
import com.fraudlab.serde.JsonSerde;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;

public class StreamingJob {
    public static void main(String[] args) throws Exception {
        Config conf = ConfigFactory.load();
        String brokers = conf.getString("kafka.brokers");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000);

        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers(brokers)
                .setTopics("txns")
                .setGroupId("fraud-job")
                .setValueOnlyDeserializer(new JsonSerde<>(Transaction.class))
                .setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .build();

        DataStream<Transaction> txns = env.fromSource(source,
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<Transaction>) (e, ts) -> e.eventTime),
                "txns");

        DataStream<Alert> velocity = txns.keyBy(t -> t.cardId)
                .process(new VelocityRuleFn(Time.minutes(10),
                        conf.getInt("windows.velocity.threshold")));

        DataStream<Alert> zscore = txns.keyBy(t -> t.userId)
                .process(new ZScoreFn(conf.getDouble("thresholds.zscore")));

        DataStream<Alert> impossible = ImpossibleTravelCEP.apply(txns, 3000,
                Time.minutes(30));

        DataStream<Alert> all = velocity.union(zscore).union(impossible)
                .map(new RiskBlenderFn(conf.getDouble("thresholds.blended.challenge"),
                        conf.getDouble("thresholds.blended.block")));

        all.addSink(new AlertSink(brokers, "alerts"));

        env.execute("fraud-lab");
    }
}
