package it.bigdata.hadoop.esercizio1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(), "esercizio1.1");

		// Set the Jar by finding where a given class came from
		job.setJarByClass(Driver.class);

		job.setMapperClass(MyMapper.class); // Set the Mapper for the job.
		
//		job.setSortComparatorClass(cls);
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
/* concatena un map task dopo la reduce */
//		ChainReducer.addMapper(job, klass, inputKeyClass, inputValueClass, outputKeyClass, outputValueClass, byValue, mapperConf);
		/*
		 * Secondo me la soluzione per fare l'ordinamento � questa: 
		 * 1 Chaining MapReduce jobs in a sequence 
		 * Though you can execute the two jobs
		 * manually one after the other, it�s more convenient to automate the
		 * execution sequence. You can chain MapReduce jobs to run sequen�
		 * tially, with the output of one MapReduce job being the input to the
		 * next. Chaining MapReduce jobs is analogous to Unix pipes .
		 * mapreduce-1 | mapreduce-2 | mapreduce-3 | ...
		 * 
		 * Chaining MapReduce jobs sequentially is quite straightforward. Recall
		 * that a driver sets up a JobConf object with the configuration
		 * parameters for a MapReduce job and passes the JobConf object to
		 * JobClient.runJob() to start the job. As Job� Client.runJob() blocks
		 * until the end of a job, chaining MapReduce jobs involves calling the
		 * driver of one MapReduce job after another. The driver at each job
		 * will have to create a new JobConf object and set its input path to be
		 * the output path of the previous job. You can delete the intermediate
		 * data generated at each step of the chain at the end.
		 */

		/* da qui in poi è un tentativo */
		//
		// Job job2 = Job.getInstance(new Configuration(),
		// "esercizio1.1 chain");
		//
		// job.setSortComparatorClass(MySchifo.class); //ordinamento decrescente
		// nella fase di shuffle&sort
		//
		// job2.setMapperClass(Mapper.class); //questo mapper dovrà solo
		// invertire k,v in v,k
		// job2.setReducerClass(Reducer.class); //non deve fare niente a parte
		// riportare in forma k,v context.write(words, keySum);
		//
		// /* output del tipo (testo, intero) */
		// job.setOutputKeyClass(Text.class); // Set the key class for the job
		// output data.
		// job.setOutputValueClass(IntWritable.class); // Set the value class
		// for the job output data.
		//
		// job.waitForCompletion(true); // Submit the job to the cluster and
		// wait
		// // for it to finish.
		// // Se il parametro è true, print the
		// // progress to the user
		//
	}
}
