package com.fraudlab.stats;

import com.fraudlab.model.Alert;
import com.fraudlab.model.Transaction;
import com.fraudlab.util.Stats;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ZScoreFn extends KeyedProcessFunction<String, Transaction, Alert> {
    private final double threshold;
    private transient ValueState<Stats> state;

    public ZScoreFn(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Stats> desc = new ValueStateDescriptor<>("stats", TypeInformation.of(Stats.class));
        state = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(Transaction value, Context ctx, Collector<Alert> out) throws Exception {
        Stats s = state.value();
        if (s == null) s = new Stats();
        double mean = s.mean();
        double std = s.std();
        double z = std > 0 ? (value.amount - mean)/std : 0;
        if (Math.abs(z) > threshold) {
            out.collect(new Alert(value.txnId, "zscore", Math.abs(z), "zscore="+z, "CHALLENGE", value.eventTime, ""));
        }
        s.add(value.amount);
        state.update(s);
    }
}
