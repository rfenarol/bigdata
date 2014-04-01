package it.bigdata.hadoop.esercizio1;

import java.io.IOException;

import org.apache.hadoop.io.*;

public class MySchifo implements RawComparator<IntWritable>{

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

        try {

            String v1 = Text.decode(b1, s1, l1);
            String v2 = Text.decode(b2, s2, l2);

            int v1Int = Integer.valueOf(v1.trim());
            int v2Int = Integer.valueOf(v2.trim());

            return (v1Int < v2Int) ? -1 : ((v1Int > v2Int) ? 1 : 0);

        }
        catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
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
