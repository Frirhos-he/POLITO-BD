package it.polito.bigdata.hadoop.lab;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class kwh2021 implements
org.apache.hadoop.io.Writable {
  long count20;
  long count21;

    public kwh2021(int i, int j) {
        this.count20 = i;
        this.count21 = j;
    }
  	@Override
	public void readFields(DataInput in) throws IOException {
		count20 = in.readLong();
		count21 = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(count20);
		out.writeLong(count21);

	}

	public String toString() {
		return count20 +" "+count21;
	}

}
