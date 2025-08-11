package com.fraudlab.rules;

import com.fraudlab.model.Alert;
import com.fraudlab.model.Transaction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class VelocityRuleFn extends KeyedProcessFunction<String, Transaction, Alert> {
    private final long windowMs;
    private final int threshold;
    private transient ListState<Long> timestamps;

    public VelocityRuleFn(Time window, int threshold) {
        this.windowMs = window.toMilliseconds();
        this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<Long> desc = new ListStateDescriptor<>("ts", TypeInformation.of(Long.class));
        timestamps = getRuntimeContext().getListState(desc);
    }

    @Override
    public void processElement(Transaction value, Context ctx, Collector<Alert> out) throws Exception {
        long ts = value.eventTime;
        timestamps.add(ts);
        long cutoff = ts - windowMs;
        int count = 0;
        for (Long t : timestamps.get()) {
            if (t >= cutoff) count++;
        }
        if (count >= threshold) {
            out.collect(new Alert(value.txnId, "velocity", count, "velocity threshold", "CHALLENGE", ts, "count="+count));
        }
    }
}
