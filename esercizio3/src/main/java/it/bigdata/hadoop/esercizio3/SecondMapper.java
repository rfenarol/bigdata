package it.bigdata.hadoop.esercizio3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper extends Mapper<LongWritable, Text, UserCoupleWritable, Text> {
	private String firstAndSecond;
	private Text interest = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tokenizer;
		String line = value.toString();
		tokenizer = new StringTokenizer(line);

		UserCoupleWritable couple;
		/* primo token: c1,c2 */
		if(tokenizer.hasMoreTokens()) 
			firstAndSecond = tokenizer.nextToken();
		String[] coppia = firstAndSecond.split(",");
		couple = new UserCoupleWritable(coppia[0],coppia[1]);
		
		/* secondo token: interesse */
		if(tokenizer.hasMoreTokens()) 
			interest.set(tokenizer.nextToken());
		context.write(couple, interest);

	}

}
