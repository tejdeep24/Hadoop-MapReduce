package _38SecondarySortTopScore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class MyGroupComparator extends WritableComparator{
	
	private static final Logger LOGGER = Logger.getLogger(MyGroupComparator.class);
	
	public MyGroupComparator() {
		
		super(Text.class,true);
		LOGGER.info("MyGroupComparator()");
	}
	
	@SuppressWarnings("rawtypes")
	public  int compare(WritableComparable statemarks1,WritableComparable statemarks2)
	{
		LOGGER.info("GroupCompare method");
		String StateMarks1 = ((Text)statemarks1).toString().trim();
		String StateMarks2 = ((Text)statemarks2).toString().trim();
		String details1[] = StateMarks1.split(":");
		String State1 = details1[0];
		String details2[] = StateMarks2.split(":");
		String State2 = details2[0];
		LOGGER.info(State1+":::"+State2);
		return (State1.hashCode()&Integer.MAX_VALUE)-(State2.hashCode()&Integer.MAX_VALUE);
	}
}
