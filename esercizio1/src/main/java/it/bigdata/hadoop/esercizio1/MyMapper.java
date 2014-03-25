package it.bigdata.hadoop.esercizio1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer tokenizer;
		String allText = value.toString();
		String[] lines = allText.split("\n");

		for (int i = 0; i < lines.length; i++) {
			tokenizer = new StringTokenizer(lines[i]);

			/* leviamo il primo che è il nome utente */
			if (tokenizer.hasMoreTokens())
				tokenizer.nextToken();

			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}

	}

}
