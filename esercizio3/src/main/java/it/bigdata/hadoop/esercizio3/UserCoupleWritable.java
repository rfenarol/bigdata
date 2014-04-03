package it.bigdata.hadoop.esercizio3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class UserCoupleWritable implements WritableComparable<UserCoupleWritable>{
	private Text u1;
	private Text u2;
	
	public UserCoupleWritable(Text user1, Text user2){
		setU1(user1);
		setU2(user2);
	}
	
	public Text getU1() {
		return u1;
	}
	public void setU1(Text u1) {
		this.u1 = u1;
	}
	public Text getU2() {
		return u2;
	}
	public void setU2(Text u2) {
		this.u2 = u2;
	}
	
	/* da rivedere
	 * si puo' definire meglio, magari facendo un equals tra le stringhe, senza usare il Text 
	 */
	public boolean equals (Object o){
		if (o instanceof UserCoupleWritable){
			UserCoupleWritable c = (UserCoupleWritable) o;
			return (u1.compareTo(c.getU1())==0 && u2.compareTo(c.getU2())==0) || 
					(u1.compareTo(c.getU2())==0 && u2.compareTo(c.getU1()) == 0);
		}
		
		return false;	
	}

	public int hashCode(){
		return u1.hashCode() + u2.hashCode();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(u1.toString());
		out.writeUTF(u2.toString());
	}

	public void readFields(DataInput in) throws IOException {
		u1 = new Text(in.readUTF());
		u2 = new Text(in.readUTF());		
	}

	/* bisogna definire un metodo compareTo per fare un ordinamento sulle coppie */
	public int compareTo(UserCoupleWritable c) {
		int cmp = u1.compareTo(c.getU1());
		if (cmp != 0) {
			return cmp;
		}
		return u2.compareTo(c.getU2());
	}
	
	public String toString (){
		String s = this.u1.toString() + "," + this.u2.toString();
		return s;
	}
}
