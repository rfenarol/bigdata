package it.bigdata.hadoop.esercizio1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();		
//		ControlledJob cjob = new ControlledJob(conf);
//		cjob.setJobName("esercizio1.1-first");
	
		Job job = Job.getInstance(conf, "esercizio1.1-first");
		job.setJarByClass(Driver.class);
		job.setMapperClass(FirstMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(FirstReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0])); // file su cui fare l'elaborazione
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // directory di output dei risultati INTERMEDI
		
		
		/* definisco il secondo job, per l'ordinamento */
		Configuration conf2 = new Configuration();		
//		ControlledJob cjob2 = new ControlledJob(conf2);
//		cjob2.setJobName("esercizio1.1-jar02");
		
//		cjob2.addDependingJob(cjob);	//inizia il job2 solo dopo che il job e' finito
		Job job2 = Job.getInstance(conf2, "esercizio1.1-second");
		job2.setJarByClass(Driver.class);
		job2.setMapperClass(SecondMapper.class);
		job2.setReducerClass(SecondReducer.class);
		
		job2.setSortComparatorClass(DescendingComparator.class);
		
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[1])); 	// output del primo job come input per il secondo
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));	//directory di output dei risultati ordinati in modo decrescente
	
		job.waitForCompletion(true);
		job2.waitForCompletion(true);
	}
	
}
