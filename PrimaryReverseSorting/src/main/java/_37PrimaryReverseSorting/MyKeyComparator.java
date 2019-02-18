package _37PrimaryReverseSorting;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class MyKeyComparator extends WritableComparator {
	
	private static final Logger LOGGER = Logger.getLogger(MyKeyComparator.class);
	
	public MyKeyComparator() {
		super(IntWritable.class,true);
		LOGGER.info("MyKeyComparator()");
	}
	
	public int compare(WritableComparable Yearone,WritableComparable YearTwo)
	{
		LOGGER.info("MyKeyComparator.compare(-,-)");
		LOGGER.info(Yearone+"<=>"+YearTwo);
		int year1 = ((IntWritable)Yearone).get();
		int year2 = ((IntWritable)YearTwo).get();
		
		return -(year1-year2);
	}
	
	

}
