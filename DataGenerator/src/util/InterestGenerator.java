package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

public class InterestGenerator {
	
	private ArrayList<String> interests;
	
	public InterestGenerator(String fileName) throws IOException{
		this.interests = new ArrayList<String>();
		File name = new File(fileName);
		if (name.isFile()) {
			try {
				BufferedReader input = new BufferedReader(new FileReader(name));
				String text;
				while ((text = input.readLine()) != null)
					interests.add(text);
				input.close();
			} catch (IOException ioException) {
				ioException.printStackTrace();
			}
		}	
		
	}
	
	public String getSingleInterest(){
		int index = new Random().nextInt(this.interests.size());
        String single_interest = this.interests.get(index);
        return single_interest;
	}
	
	public String getMutipleInterest(int n){
		String multiple_interest = "";
		HashSet<String> hs = new HashSet<String>();
		for (int i = 0; i<n; i++)	
			hs.add(getSingleInterest());
		for (String text: hs)
             multiple_interest = multiple_interest + " " + text;

        return multiple_interest;
	}

}
