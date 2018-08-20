package com.ssy.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//按照年、月、温度的顺序进行比较
public class TSorter extends  WritableComparator{
	
	
	public TSorter() {
		//调用父类构造器，创建两个TQ类型的对象，为后续比较做准备，防止空指针异常
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
			int c2 = Integer.compare(t1.getMonth(), t2.getMonth());
			if(c2 == 0){
				return  -Integer.compare(t1.getWd(), t2.getWd());
			}
			return c2;
		}
		return  c1;
	}
	

}
