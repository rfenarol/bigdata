package it.bigdata.hadoop.esercizio3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text interest = new Text();
	private Text user = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		StringTokenizer tokenizer;
		String line = value.toString();
		tokenizer = new StringTokenizer(line);

		if(tokenizer.hasMoreTokens()) {
			user.set(tokenizer.nextToken());
		}
		while(tokenizer.hasMoreTokens()) {
			interest.set(tokenizer.nextToken());
			context.write(interest, user);
		}

	}

}
