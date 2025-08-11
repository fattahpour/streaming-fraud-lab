package com.fraudlab.util;

public class Stats {
    private long n = 0;
    private double mean = 0.0;
    private double m2 = 0.0;

    public void add(double x) {
        n++;
        double delta = x - mean;
        mean += delta/n;
        m2 += delta*(x-mean);
    }

    public double mean() { return mean; }
    public double variance() { return n>1 ? m2/(n-1) : 0.0; }
    public double std() { return Math.sqrt(variance()); }
}
