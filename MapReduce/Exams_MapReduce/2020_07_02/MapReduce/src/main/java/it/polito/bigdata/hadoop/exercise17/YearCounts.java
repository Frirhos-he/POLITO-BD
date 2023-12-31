package it.polito.bigdata.hadoop.exercise17;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class YearCounts implements Writable {
    private Integer count2018;
    private Integer count2019;



    public YearCounts( Integer c2018,Integer c2019) {

        count2018 = c2018;
        count2019 = c2019;
        
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(count2018);
        dataOutput.writeInt(count2019);
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {

        count2018 = dataInput.readInt();
        count2019 = dataInput.readInt();
    }


    public int getCount2018() {
        return count2018.intValue();
    }

    public void setCount2018(int count2018) {
        this.count2018 = count2018;
    }

    // Getters and setters for count2019
    public int getCount2019() {
        return count2019.intValue();
    }

    public void setCount2019(int count2019) {
        this.count2019 =count2019;
    }




}
