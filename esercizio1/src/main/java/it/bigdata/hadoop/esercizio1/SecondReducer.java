package it.bigdata.hadoop.esercizio1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
	private Text interessi = new Text();
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		
		for (Text interesse : values) {
			interessi.set(interesse.toString());
			context.write(interessi, key);
		}
	}

}
