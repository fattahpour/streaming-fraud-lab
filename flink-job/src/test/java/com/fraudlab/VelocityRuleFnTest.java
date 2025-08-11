package com.fraudlab;

import com.fraudlab.model.Alert;
import com.fraudlab.model.Transaction;
import com.fraudlab.rules.VelocityRuleFn;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

public class VelocityRuleFnTest {
    @Test
    public void testVelocity() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(0);
        Transaction t1 = new Transaction("t1", 0, "c1", "u1", "m1", "5411", 1.0, "USD", "ip", "d", 0,0,"US","APPROVED","e");
        Transaction t2 = new Transaction("t2", 1000, "c1", "u1", "m1", "5411", 1.0, "USD", "ip", "d", 0,0,"US","APPROVED","e");
        Transaction t3 = new Transaction("t3", 2000, "c1", "u1", "m1", "5411", 1.0, "USD", "ip", "d", 0,0,"US","APPROVED","e");
        var stream = env.fromElements(t1,t2,t3)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Transaction>forMonotonousTimestamps()
                        .withTimestampAssigner((e,ts)->e.eventTime))
                .keyBy(t->t.cardId)
                .process(new VelocityRuleFn(Time.minutes(1),3));
        Iterator<Alert> it = DataStreamUtils.collect(stream);
        Alert a = it.next();
        assertEquals("velocity", a.detector);
    }
}
