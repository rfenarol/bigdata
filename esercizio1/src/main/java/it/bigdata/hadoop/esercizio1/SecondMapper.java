package it.bigdata.hadoop.esercizio1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	private IntWritable occorrenza;
	private Text interesse = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tokenizer;
		String line = value.toString();
		tokenizer = new StringTokenizer(line);
		
		/* linea del tipo 'interesse 12' */
		if (tokenizer.hasMoreTokens())
			interesse.set(tokenizer.nextToken());
		if (tokenizer.hasMoreTokens())
			occorrenza = new IntWritable( Integer.parseInt(tokenizer.nextToken()) );
			
		context.write(occorrenza, interesse);
	}

}
