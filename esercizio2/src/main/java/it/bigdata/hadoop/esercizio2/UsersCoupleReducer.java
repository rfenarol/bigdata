package it.bigdata.hadoop.esercizio2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UsersCoupleReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int i=0;
		Iterator<Text> extIt = values.iterator();
		while(extIt.hasNext()){
			int j=0;
			Text first = extIt.next();
			first.set(first.toString()+",");
			Iterator<Text> intIt = values.iterator();
			while(j<i+1 && intIt.hasNext()){
				intIt.next();
				j+=1;
			}
			while(intIt.hasNext()){
				Text second = intIt.next();
				context.write(first, second);
			}
			i+=1;
		}
	}
}
