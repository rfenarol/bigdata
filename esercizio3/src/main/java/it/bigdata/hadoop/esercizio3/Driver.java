package it.bigdata.hadoop.esercizio3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "esercizio1.3-first");
		job.setJarByClass(Driver.class);
		job.setMapperClass(FirstMapper.class);
		job.setReducerClass(FirstReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Configuration conf2 = new Configuration();		
		Job job2 = Job.getInstance(conf2, "esercizio1.3-second");
		job2.setJarByClass(Driver.class);
		job2.setMapperClass(SecondMapper.class);
		job2.setReducerClass(SecondReducer.class);
		
		job2.setOutputKeyClass(UserCoupleWritable.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[1])); 	// output del primo job come input per il secondo
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));	//directory di output dei risultati ordinati in modo decrescente
	
		job.waitForCompletion(true);
		job2.waitForCompletion(true);
	}

}
