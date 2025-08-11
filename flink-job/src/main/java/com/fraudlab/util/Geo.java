package com.fraudlab.util;

public class Geo {
    private static final double R = 6371e3; // meters
    public static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double dphi = Math.toRadians(lat2 - lat1);
        double dlambda = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dphi/2)*Math.sin(dphi/2) +
                Math.cos(phi1)*Math.cos(phi2)*Math.sin(dlambda/2)*Math.sin(dlambda/2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c / 1000.0; // km
    }
}
