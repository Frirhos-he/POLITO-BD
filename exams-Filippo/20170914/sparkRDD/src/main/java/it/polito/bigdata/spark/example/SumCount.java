package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class SumCount implements Serializable {
    int sum;
    int count;

    public SumCount(int sum, int count) {
        this.sum = sum;
        this.count = count;
    }
}
