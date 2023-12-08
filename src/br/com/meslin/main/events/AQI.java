package br.com.meslin.main.events;

public class AQI {
    private final int region;
    private final double concentration;

    public AQI(int region, double concentration) {
        this.region = region;
        this.concentration = concentration;
    }

    public double getConcentration() {
        return concentration;
    }

    public int getRegion() {
        return region;
    }
}
