package it.bigdata.hadoop.esercizio1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();		
		ControlledJob cjob = new ControlledJob(conf);
		cjob.setJobName("esercizio1.1-jar01");
	
		Job job = cjob.getJob();
		job.setJarByClass(Driver.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0])); // file su cui fare l'elaborazione
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // directory di output dei risultati INTERMEDI
		
		
		/* definisco il secondo job, per l'ordinamento */
		Configuration conf2 = new Configuration();		
		ControlledJob cjob2 = new ControlledJob(conf2);
		cjob2.setJobName("esercizio1.1-jar02");
		
		cjob2.addDependingJob(cjob);	//inizia il job2 solo dopo che il job e' finito
		Job job2 = cjob2.getJob();
		job2.setJarByClass(Driver.class);
		job2.setMapperClass(MySecondMapper.class);
		job2.setReducerClass(MySecondReducer.class);
		
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[1])); 	// output del primo job come input per il secondo
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));	//directory di output dei risultati ordinati in modo decrescente
	
		JobControl control = new JobControl("interessi ordinati per numero di occorrenze");
		control.addJob(cjob);
		control.addJob(cjob2);
		control.run();
		
		job.waitForCompletion(true);
		job2.waitForCompletion(true);
		
		//TODO
		/* problemi da risolvere:
		 * 1) verificare che il sorting non sia lessicografico ma giusto
		 * 2) ora è in ordine crescente, serve farlo in ordine decrescente
		 * 3) cambiando il modo di eseguire i job, il processo su terminale rimane appeso e non termina
		 * come al solito, e non mostra neanche le percentuali di map e reduce.
		 */
	}
	
	
	/*
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "esercizio1.1");
		
		ControlledJob cjob = new ControlledJob(conf);
		cjob.setJobName("esercizio1.1");
		

		// Set the Jar by finding where a given class came from
		job.setJarByClass(Driver.class);

		job.setMapperClass(MyMapper.class); // Set the Mapper for the job.
		job.setCombinerClass(Combiner.class);

		job.setReducerClass(MyReducer.class); // Set the Reducer for the job.
		/*ChainReducer.setReducer(job, MyReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);
		ChainReducer.addMapper(job, MySecondMapper.class, Text.class, IntWritable.class, IntWritable.class, Text.class, conf);
		job.setSortComparatorClass(MySchifo.class);
		*/
	/*
		FileInputFormat.addInputPath(job, new Path(args[0])); // file su cui fare l'elaborazione
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // directory di output dei risultati

		/* output del tipo (testo, intero)*/ /*
		job.setOutputKeyClass(Text.class); // Set the key class for the job output data.
		job.setOutputValueClass(IntWritable.class); // Set the value class for the job output data.

		job.waitForCompletion(true); // Submit the job to the cluster and wait for it to finish.
										// Se il parametro è true, print the progress to the user

		/* da qui in poi è un tentativo */
		/*
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
		// /* output del tipo (testo, intero) */ /*
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
	}*/
}
