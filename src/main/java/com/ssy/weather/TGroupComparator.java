package com.ssy.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TGroupComparator extends  WritableComparator{

	public TGroupComparator() {
		super(TQ.class,true);
	}
	
	TQ t1 = null;
	TQ t2 = null;
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		t1= (TQ)a;
		t2= (TQ)b;
		
		int c1 = Integer.compare(t1.getYear(),t2.getYear());
		if(c1 == 0){
			return  Integer.compare(t1.getMonth(), t2.getMonth());
		}
		return  c1;
	}
	
	
	
	
}
