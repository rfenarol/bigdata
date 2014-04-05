package it.bigdata.hadoop.esercizio3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class UserCoupleWritable implements WritableComparable<UserCoupleWritable>{
	private String u1;
	private String u2;
	
	public UserCoupleWritable(){
	}
	
	public UserCoupleWritable(String user1, String user2){
		setU1(user1);
		setU2(user2);
	}
	
	public String getU1() {
		return u1;
	}
	public void setU1(String u1) {
		this.u1 = u1;
	}
	public String getU2() {
		return u2;
	}
	public void setU2(String u2) {
		this.u2 = u2;
	}
	
	public boolean equals (Object o){
		if (o instanceof UserCoupleWritable){
			UserCoupleWritable c = (UserCoupleWritable) o;
			return (u1.equals(c.getU1()) && u2.equals(c.getU2())) || 
					(u1.equals(c.getU2()) && u2.equals(c.getU1()));
		}
		return false;	
	}

	public int hashCode(){
		return u1.hashCode() + u2.hashCode();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(u1);
		out.writeUTF(u2);
	}

	public void readFields(DataInput in) throws IOException {
		u1 = in.readUTF();
		u2 = in.readUTF();		
	}

	public int compareTo(UserCoupleWritable c) {
		int test1 = 0;
		int test2 = 0;
		
		test1 = u1.compareTo(c.getU1());
		if (test1 == 0)
			test1 = u2.compareTo(c.getU2());
		
		test2 = u1.compareTo(c.getU2());
		if (test2 == 0) 
			test2 = u2.compareTo(c.getU1());
		
		if (test1==0 || test2 == 0)
			return 0;
		else {
			if (test1 !=0)
				return test1;
			if (test2 !=0)
				test1 = test2;
		}
		return test1;
	}
	
	public String toString (){
		String s = this.u1 + "," + this.u2;
		return s;
	}
}
