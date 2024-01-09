package it.polito.bigdata.hadoop.exercise11;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumCount implements 
org.apache.hadoop.io.Writable  { //implements Comparable<SumCount>
	private float sum = 0;
	private int count = 0;

	public SumCount(int sum, String count) {
        this.sum = sum;
        this.count = count;
    }
	public float getSum() {
		return sum;
	}

	public void setSum(float sumValue) {
		sum = sumValue;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int countValue) {
		count = countValue;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sum = in.readFloat();
		count = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(sum);
		out.writeInt(count);
		//writeUTF(word);
	}

	public String toString() {
		String formattedString = new String("" + (float) sum / count);

		return formattedString;
	}

	/*
	@Override
	public int compareTo(WordCountWritable other) {

		if (this.count.compareTo(other.getCount()) != 0) {
			return this.count.compareTo(other.getCount());
		} else { // if the count values of the two words are equal, the
					// lexicographical order is considered
			return this.word.compareTo(other.getWord());
		}

	}
	 */
}


/*
 * package it.polito.bigdata.hadoop.lab;

import java.util.Vector;
This class is used to store the top-k elements of a set of objects of type T.
 T is a class implementing the Comparable interface 

public class TopKVector<T extends Comparable<T>> {

	private Vector<T> localTopK;
	private Integer k;

	// It is used to create an empty TopKVector object.
	// k = number of top-k objects to store in this TopKVector object
	public TopKVector(int k) {
		this.localTopK = new Vector<T>();
		this.k = k;
	}

	public int getK() {
		return this.k;
	}

	// It is used to retrieve the vector containing the top-k objects among the
	// inserted ones
	public Vector<T> getLocalTopK() {
		return this.localTopK;
	}

	/*
	 * It is used to insert a new element in the current top-k vector. The new
	 * element is inserted in the this.localTopK vector if and only if it is in
	 * the top-k objects.
	 
	public void updateWithNewElement(T currentElement) {
		if (localTopK.size() < k) { // There are less than k objects in
									// localTopK. Add the current element at the
									// end of localTopK
			localTopK.addElement(currentElement);

			// Sort the objects in localTopk
			sortAfterInsertNewElement();
		} else {
			// There are already k elements
			// Check if the current one is better than the least one
			if (currentElement.compareTo(localTopK.elementAt(k - 1)) > 0) {
				// The current element is better than the least object in
				// localTopK
				// Substitute the last object of localTopK with the current
				// object
				localTopK.setElementAt(currentElement, k - 1);

				// Sort the objects in localTopk
				sortAfterInsertNewElement();
			}
		}
	}

	private void sortAfterInsertNewElement() {
		// The last object is the only one that is potentially not in the right
		// position
		T swap;

		for (int pos = localTopK.size() - 1; pos > 0
				&& localTopK.elementAt(pos).compareTo(localTopK.elementAt(pos - 1)) > 0; pos--) {
			swap = localTopK.elementAt(pos);
			localTopK.setElementAt(localTopK.elementAt(pos - 1), pos);
			localTopK.setElementAt(swap, pos - 1);
		}
	}

}

 */