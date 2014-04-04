package it.bigdata.hadoop.esercizio2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		List<String> names = new ArrayList<String>();
		for (Text t : values)
			names.add(t.toString());
		int i,j;
		for (i = 0; i<names.size(); i++){
			for(j=i+1; j< names.size(); j++){
				Text first = new Text();
				Text second = new Text();
				String f = names.get(i) + ",";
				first.set(f);
				second.set(names.get(j));
				context.write(first, second);
			}
		}
	}

}
