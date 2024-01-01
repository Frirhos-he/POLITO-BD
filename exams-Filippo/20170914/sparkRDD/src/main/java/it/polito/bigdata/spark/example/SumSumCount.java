package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class SumSumCount implements Serializable{
    int count;
    double sumOverload;
    int nCancelled;

    public SumSumCount(int count, double sumOverload, int nCancelled) {
        this.count = count;
        this.sumOverload = sumOverload;
        this.nCancelled = nCancelled;
    }
}
