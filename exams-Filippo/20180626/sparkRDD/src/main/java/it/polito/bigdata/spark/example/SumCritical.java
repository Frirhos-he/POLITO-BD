package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class SumCritical implements Serializable {
    int cpuCriticalHoursMax;
    int cpuCriticalHoursMin;

    public SumCritical(int cpuCriticalHoursMax, int cpuCriticalHoursMin) {
        this.cpuCriticalHoursMax = cpuCriticalHoursMax;
        this.cpuCriticalHoursMin = cpuCriticalHoursMin;
    }
}
