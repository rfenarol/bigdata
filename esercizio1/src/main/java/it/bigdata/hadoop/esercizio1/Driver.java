package it.bigdata.hadoop.esercizio1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(), "esercizio1.1");

		// Set the Jar by finding where a given class came from
		job.setJarByClass(Driver.class);

		job.setMapperClass(MyMapper.class); // Set the Mapper for the job.
		job.setReducerClass(MyReducer.class); // Set the Reducer for the job.

		FileInputFormat.addInputPath(job, new Path(args[0])); // file su cui
																// fare
																// l'elaborazione
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // directory di
																// output dei
																// risultati

		/* output del tipo (testo, intero) */
		job.setOutputKeyClass(Text.class); // Set the key class for the job
											// output data.
		job.setOutputValueClass(IntWritable.class); // Set the value class for
													// the job output data.

		job.waitForCompletion(true); // Submit the job to the cluster and wait
										// for it to finish.
										// Se il parametro è true, print the
										// progress to the user
		
		
		/* da qui in poi è un tentativo */
//		
//		Job job2 = Job.getInstance(new Configuration(), "esercizio1.1 chain");
//		
//		job.setSortComparatorClass(MySchifo.class); //ordinamento decrescente nella fase di shuffle&sort
//		
//		job2.setMapperClass(Mapper.class); //questo mapper dovrà solo invertire k,v in v,k
//		job2.setReducerClass(Reducer.class); //non deve fare niente a parte riportare in forma k,v context.write(words, keySum);
//		
//		/* output del tipo (testo, intero) */
//		job.setOutputKeyClass(Text.class); // Set the key class for the job output data.
//		job.setOutputValueClass(IntWritable.class); // Set the value class for the job output data.
//
//		job.waitForCompletion(true); // Submit the job to the cluster and wait
//										// for it to finish.
//										// Se il parametro è true, print the
//										// progress to the user
//		
	}
}
		
