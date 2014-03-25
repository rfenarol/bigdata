package util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Random;

public class ItemGenerator {
   
	private NameGenerator ng;
	private InterestGenerator ig;
	
	public ItemGenerator(String syllable_file, String interest_file) throws IOException{
		this.ng = new NameGenerator(syllable_file);
		this.ig = new InterestGenerator(interest_file);
	}
	
	public void generate(String fileName, int num_items, int max_num_syllable, int max_num_interest){
		
		try
	     {
	          FileOutputStream stream = new FileOutputStream(fileName);
	          PrintStream data = new PrintStream(stream);
	          for(int i=0;i<num_items;i++)
	          {
	        	  int num_syllable = new Random().nextInt(max_num_syllable);
	        	  if (num_syllable == 0) num_syllable++;
	        	  int num_interest = new Random().nextInt(max_num_interest);
	        	  if (num_interest == 0) num_interest++;
	        	  String name = ng.compose(num_syllable);
	        	  String interests = ig.getMutipleInterest(num_interest);
	        	  data.println(name+interests);
	          }
	          data.close();
	          stream.close();
	      }
	      catch (IOException e)
	      {
	          System.out.println("Error: " + e);
	          System.exit(1);
	      }
		
		
	}
	
}
