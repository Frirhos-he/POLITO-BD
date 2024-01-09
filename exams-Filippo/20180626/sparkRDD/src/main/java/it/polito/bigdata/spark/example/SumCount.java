package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class SumCount implements Serializable {
    double cpuSum;
    double ramSum;
    int count;

    public SumCount (double cpuSum, double ramSum, int count) {
        this.cpuSum = cpuSum;
        this.ramSum = ramSum;
        this.count = count;
    }
}
