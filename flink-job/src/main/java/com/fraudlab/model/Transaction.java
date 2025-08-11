package com.fraudlab.model;

import java.io.Serializable;

public class Transaction implements Serializable {
    public String txnId;
    public long eventTime;
    public String cardId;
    public String userId;
    public String merchantId;
    public String mcc;
    public double amount;
    public String currency;
    public String ip;
    public String deviceId;
    public double lat;
    public double lon;
    public String country;
    public String status;
    public String channel;

    public Transaction() {}

    public Transaction(String txnId, long eventTime, String cardId, String userId,
                       String merchantId, String mcc, double amount, String currency,
                       String ip, String deviceId, double lat, double lon,
                       String country, String status, String channel) {
        this.txnId = txnId;
        this.eventTime = eventTime;
        this.cardId = cardId;
        this.userId = userId;
        this.merchantId = merchantId;
        this.mcc = mcc;
        this.amount = amount;
        this.currency = currency;
        this.ip = ip;
        this.deviceId = deviceId;
        this.lat = lat;
        this.lon = lon;
        this.country = country;
        this.status = status;
        this.channel = channel;
    }
}
