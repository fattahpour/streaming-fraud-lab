package com.fraudlab.model;

import java.io.Serializable;

public class Alert implements Serializable {
    public String txnId;
    public String detector;
    public double score;
    public String message;
    public String action;
    public long eventTime;
    public String details;

    public Alert() {}

    public Alert(String txnId, String detector, double score, String message, String action, long eventTime, String details) {
        this.txnId = txnId;
        this.detector = detector;
        this.score = score;
        this.message = message;
        this.action = action;
        this.eventTime = eventTime;
        this.details = details;
    }
}
