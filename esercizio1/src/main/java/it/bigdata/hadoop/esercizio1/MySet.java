package it.bigdata.hadoop.esercizio1;

import java.util.HashSet;

public class MySet {
	private static MySet instance;
	private HashSet<String> set;
	
	private MySet(){
		this.setSet(new HashSet<String>());
	}
	
	public MySet getInstance(){
		if (instance == null)
			instance = new MySet();
		return instance;
	}

	public HashSet<String> getSet() {
		return set;
	}

	private void setSet(HashSet<String> set) {
		this.set = set;
	}

}
