package com.fraudlab.cep;

import com.fraudlab.model.Alert;
import com.fraudlab.model.Transaction;
import com.fraudlab.util.Geo;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class ImpossibleTravelCEP {
    public static DataStream<Alert> apply(DataStream<Transaction> input, double distanceKm, Time within) {
        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("first")
                .where(new IterativeCondition<>() {
                    @Override
                    public boolean filter(Transaction value, Context<Transaction> ctx) {
                        return true;
                    }
                })
                .next("second").where(new IterativeCondition<>() {
                    @Override
                    public boolean filter(Transaction value, Context<Transaction> ctx) {
                        return true;
                    }
                }).within(within);

        return CEP.pattern(input.keyBy(t -> t.userId), pattern)
                .select((PatternSelectFunction<Transaction, Alert>) map -> {
                    List<Transaction> firsts = map.get("first");
                    List<Transaction> seconds = map.get("second");
                    Transaction a = firsts.get(0);
                    Transaction b = seconds.get(0);
                    double dist = Geo.haversine(a.lat, a.lon, b.lat, b.lon);
                    if (dist > distanceKm) {
                        return new Alert(b.txnId, "impossible_travel", dist, "distance=" + dist, "CHALLENGE", b.eventTime, "" );
                    }
                    return null;
                })
                .filter(a -> a != null);
    }
}
