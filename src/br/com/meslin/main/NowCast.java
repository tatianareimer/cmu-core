package br.com.meslin.main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class NowCast {
    static String filePath = System.getProperty("user.dir") + "/src/br/com/meslin/main/aqi/concentrations.csv";
    static double[] firstColumnDoubles;

    public static void main(String[] args) {
    }

    private static double[] getCSVData() {

        int numberOfRowsToRetrieve = 11;

        try {
            List<String> lines = Files.readAllLines(Paths.get(filePath));
            List<String> lastRows = lines.stream()
                    .skip(Math.max(0, lines.size() - numberOfRowsToRetrieve))
                    .collect(Collectors.toList());

            String[] firstColumnValues = lastRows.stream()
                    .map(row -> row.split(",")[0])
                    .toArray(String[]::new);

            firstColumnDoubles = Arrays.stream(firstColumnValues)
                    .mapToDouble(Double::parseDouble)
                    .toArray();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return firstColumnDoubles;
    }

    public static int calculateAQI(double concentrationNow) {
        double[] last11HourConcentrations = getCSVData();
        double[] last12HourConcentrations = addDataToLastPosition(last11HourConcentrations, concentrationNow);
        double maxConcentration = getMaxValue(last11HourConcentrations);
        double minConcentration = getMinValue(last12HourConcentrations);
        double diff = maxConcentration - minConcentration;
        double w = diff/maxConcentration;
        double p = 1.0 - w;
        if (p < 0.5) {
            p = 0.5;
        }
        double sumConcentration = 0;
        double sumPower = 0;
        for (int i = 0; i < last12HourConcentrations.length; i++) {
            sumConcentration += last12HourConcentrations[i] * Math.pow(p, i);
        }
        for (int i = 0; i < last12HourConcentrations.length; i++) {
            sumPower += Math.pow(p, i);
        }
        double nowcast = sumConcentration/sumPower;
        double concHi;
        double concLo;
        double aqiHi;
        double aqiLo;

        if (nowcast <= 25.0) {
            concHi = 25.0;
            concLo = 0.0;
            aqiHi = 40.0;
            aqiLo = 0.0;
        } else if (nowcast > 25.0 && nowcast <= 50.0) {
            concHi = 50.0;
            concLo = 25.1;
            aqiHi = 80.0;
            aqiLo = 41.0;
        } else if (nowcast > 50.0 && nowcast <= 75.0) {
            concHi = 75.0;
            concLo = 50.1;
            aqiHi = 120.0;
            aqiLo = 81.0;
        } else if (nowcast > 75.0 && nowcast <= 125.0) {
            concHi = 125.0;
            concLo = 75.1;
            aqiHi = 200.0;
            aqiLo = 121.0;
        } else {
            concHi = 300;
            concLo = 125.1;
            aqiHi = 250.0;
            aqiLo = 201.0;
        }

        double iqa = ((aqiHi - aqiLo) / (concHi - concLo)) * (nowcast - concLo) + aqiLo;

        return (int) iqa;
    }

    public static String getQuality(double aqi) {
        if (aqi <= 40) {
            return "{'quality':'Boa', 'color':'00cc00'}";
        } else if (aqi > 40 && aqi <= 80) {
            return "{'quality':'Moderad', 'color':'ffff00'}";
        } else if (aqi > 80 && aqi <= 120) {
            return "{'quality':'Ruim', 'color':'ffaa71'}";
        } else if (aqi > 120 && aqi <= 200) {
            return "{'quality':'Muito Ruim', 'color':'ff0f03'}";
        } else {
            return "{'quality':'Péssima', 'color':'990066'}";
        }
    }

    private static double[] addDataToLastPosition(double[] array, double data) {
        double[] newArray = Arrays.copyOf(array, array.length + 1);

        newArray[newArray.length - 1] = data;
        return newArray;
    }

    private static double getMaxValue(double[] array) {
        return Arrays.stream(array)
                .max()
                .orElseThrow(() -> new IllegalArgumentException("Array is empty"));
    }

    private static double getMinValue(double[] array) {
        return Arrays.stream(array)
                .min()
                .orElseThrow(() -> new IllegalArgumentException("Array is empty"));
    }
}
