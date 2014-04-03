package it.bigdata.hadoop.esercizio3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<UserCoupleWritable, Text, UserCoupleWritable, Text> {
	public void reduce(UserCoupleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String s = "";
		for (Text i : values){
				s+=i.toString() + ",";				
		}
		s = s.substring(0, s.length()-1);
		Text interests = new Text(s);
		
		context.write(key, interests);
	}
}
