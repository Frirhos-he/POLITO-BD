package it.polito.bigdata.hadoop.lab;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class YearIncome implements Comparable<YearIncome>, Writable {
    private String year;
    private Double income;

    public YearIncome() {
        // Default constructor needed for Hadoop Writable
    }

    public YearIncome(String year, Double income) {
        this.year = year;
        this.income = income;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readUTF();
        income = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
        out.writeDouble(income);
    }

    public String toString() {
        return year + " " + income;
    }

    @Override
    public int compareTo(YearIncome other) {
        if (this.year.compareTo(other.year) != 0) {
            return this.year.compareTo(other.year);
        } else {
            return this.income.compareTo(other.income);
        }
    }

    // Getters and Setters
    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public Double getIncome() {
        return income;
    }

    public void setIncome(Double income) {
        this.income = income;
    }
}
