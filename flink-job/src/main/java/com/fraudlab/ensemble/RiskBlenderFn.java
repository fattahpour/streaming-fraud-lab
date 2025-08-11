package com.fraudlab.ensemble;

import com.fraudlab.model.Alert;
import org.apache.flink.api.common.functions.MapFunction;

public class RiskBlenderFn implements MapFunction<Alert, Alert> {
    private final double challenge;
    private final double block;

    public RiskBlenderFn(double challenge, double block) {
        this.challenge = challenge;
        this.block = block;
    }

    @Override
    public Alert map(Alert value) {
        String action = "ALLOW";
        if (value.score >= block) action = "BLOCK";
        else if (value.score >= challenge) action = "CHALLENGE";
        value.action = action;
        return value;
    }
}
