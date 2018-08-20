package com.ssy.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

//自定义的Key的类型，需要实现WritableComparable接口
public class TQ implements WritableComparable<TQ>   {
	
	private int year ;
	private int month;
	private int day;
	private int wd;
	
	
	
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getWd() {
		return wd;
	}

	public void setWd(int wd) {
		this.wd = wd;
	}

	
	//序列化--写
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(wd);
	}

	//反序列化--读（注意：序列化和反序列化时，属性的顺序要一致）
	@Override
	public void readFields(DataInput in) throws IOException {
		this.setYear(in.readInt());
		this.setMonth(in.readInt());
		this.setDay(in.readInt());
		this.setWd(in.readInt());
	}

	//按照年月日的顺序进行比较
	@Override
	public int compareTo(TQ that) {
		int c1=Integer.compare( this.getYear(), that.getYear() );
		if(c1==0) {
			int c2 = Integer.compare( this.getMonth(), that.getMonth() );
			if(c2==0) {
				return  Integer.compare( this.getDay(), that.getDay() );
			}
			return c2;
		}
		return c1;
	}
	
	
	
	
	
	
	

}
