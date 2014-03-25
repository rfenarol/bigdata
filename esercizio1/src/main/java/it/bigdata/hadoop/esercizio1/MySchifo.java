package it.bigdata.hadoop.esercizio1;

import org.apache.hadoop.io.*;

public class MySchifo implements RawComparator<IntWritable>{

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int compare(IntWritable o1, IntWritable o2) {
		Integer i1, i2;
		i1 = o1.get();
		i2 = o2.get();
		if (i2>i1)
			return 1;
		if (i1>i2)
			return -1;
		else
			return 0;
	}

}
