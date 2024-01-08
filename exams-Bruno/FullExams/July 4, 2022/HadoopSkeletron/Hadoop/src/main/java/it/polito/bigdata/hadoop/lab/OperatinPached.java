package it.polito.bigdata.hadoop.lab;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OperatinPached implements org.apache.hadoop.io.Writable {
    int count;
    String operatingSystem;

    public OperatinPached() {
        this.count = 0;
        this.operatingSystem = "";
    }
    @Override
	public void readFields(DataInput in) throws IOException {
		count = in.readInt();
		operatingSystem = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(count);
		out.writeUTF(operatingSystem);

	}

	public String toString() {
		return count +" "+operatingSystem;
	}
}
