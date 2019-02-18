package _38SecondaySort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class MyGroupComparator extends WritableComparator{
	
	private static final Logger LOGGER = Logger.getLogger(MyKeyComparator.class);
	
	public MyGroupComparator() {
		super(Text.class,true);
		LOGGER.info("MyGroupComparator");
	}
	
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable Yeartemp1,WritableComparable Yeartemp2)
	{
		String details1[] = ((Text)Yeartemp1).toString().split(" ");
		int year1 = Integer.parseInt(details1[0]);
		String details2[] = ((Text)Yeartemp2).toString().split(" ");
		int year2 = Integer.parseInt(details2[0]);
		return -(year1-year2);
	}

}
