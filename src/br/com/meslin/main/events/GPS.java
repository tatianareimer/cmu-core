package br.com.meslin.main.events;

public class GPS {
    private final double latitude;
    private final double longitude;
    private final int region;

    public GPS(double latitude, double longitude, int region) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.region = region;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }
}
