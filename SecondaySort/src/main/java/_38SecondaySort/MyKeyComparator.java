package _38SecondaySort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class MyKeyComparator extends WritableComparator{

	private static final Logger LOGGER = Logger.getLogger(MyKeyComparator.class);
	
	public MyKeyComparator() {
		
		super(Text.class,true);
		LOGGER.info("MyKeyComparator");
	}
	
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable Yeartemp1,WritableComparable Yeartemp2)
	{
		String details1[] = ((Text)Yeartemp1).toString().split(" ");
		int year1 = Integer.parseInt(details1[0]);
		int temp1 = Integer.parseInt(details1[1]);
		String details2[] = ((Text)Yeartemp2).toString().split(" ");
		int year2 = Integer.parseInt(details2[0]);
		int temp2 = Integer.parseInt(details2[1]);
		int cmp = year1 - year2;
		if (cmp != 0) {
			return cmp;
		}

		// if both Years are equal compare the temperature
		else {
			return -(temp1 - temp2);// highest temperature should come first
		}
		
	}
}
