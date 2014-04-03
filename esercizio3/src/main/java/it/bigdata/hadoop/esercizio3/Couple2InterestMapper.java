package it.bigdata.hadoop.esercizio3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Couple2InterestMapper extends Mapper<LongWritable, Text, UserCoupleWritable, Text> {
	private Text first = new Text();
	private Text second = new Text();
	private Text interest = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tokenizer;
		String line = value.toString();
		tokenizer = new StringTokenizer(line, ", ");

		UserCoupleWritable couple;
		if(tokenizer.hasMoreTokens()) first.set(tokenizer.nextToken());
		if(tokenizer.hasMoreTokens()) second.set(tokenizer.nextToken());
		couple = new UserCoupleWritable(first,second);
		if(tokenizer.hasMoreTokens()) interest.set(tokenizer.nextToken());
		context.write(couple, interest);

	}

}
