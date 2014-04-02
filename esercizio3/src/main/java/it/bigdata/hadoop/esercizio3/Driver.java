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
		Job job = Job.getInstance(conf, "esercizio1.2");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Interest2UsrMapper.class);
		job.setReducerClass(Interest2UserCoupleReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		/* inserire qui sotto il secondo job, che dato un elenco di coppie
		 * (coppia, interesse) restituisca un elenco di (coppia, [interessi])
		 */
		
		
		job.waitForCompletion(true);

	}

}
