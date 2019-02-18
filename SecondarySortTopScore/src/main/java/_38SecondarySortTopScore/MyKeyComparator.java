package _38SecondarySortTopScore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class MyKeyComparator extends WritableComparator{
	
	private static final Logger LOGGER = Logger.getLogger(MyKeyComparator.class);
	
	public MyKeyComparator() {
		
		super(Text.class,true);
		LOGGER.info("MyKeyComparator()");
	}
	
	@SuppressWarnings("rawtypes")
	public  int compare(WritableComparable statemarks1,WritableComparable statemarks2)
	{
		LOGGER.info("KeyCompare method");
		String StateMarks1 = ((Text)statemarks1).toString().trim();
		String StateMarks2 = ((Text)statemarks2).toString().trim();
		String details1[] = StateMarks1.split(":");
		String State1 = details1[0];
		int Marks1 = Integer.parseInt(details1[1]);
		String details2[] = StateMarks2.split(":");
		String State2 = details2[0];
		int Marks2 = Integer.parseInt(details2[1]);
		LOGGER.info(State1+":::"+Marks1+":::"+State2+":::"+Marks2);
		if(State1.equals(State2))
		{
			return -(Marks1-Marks2);
		}
		else{
			
			return State1.hashCode()-State2.hashCode();
		}
	}

}
