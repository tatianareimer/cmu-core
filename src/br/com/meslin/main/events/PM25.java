package br.com.meslin.main.events;

public class PM25 {
    private final double concentration;

    public PM25(double concentration) {
        this.concentration = concentration;
    }

    public double getConcentration() {
        return concentration;
    }
}
