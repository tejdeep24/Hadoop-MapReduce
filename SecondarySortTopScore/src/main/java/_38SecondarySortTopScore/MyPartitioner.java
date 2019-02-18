package _38SecondarySortTopScore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

public class MyPartitioner extends Partitioner<Text,Student>{

	private static final Logger LOGGER = Logger.getLogger(MyPartitioner.class);
	
	public MyPartitioner() {
		
		LOGGER.info("MyPartitioner()");
	}
	
	@Override
	public int getPartition(Text key, Student value, int numPartitions) {
		
		LOGGER.info(key+":::"+value);
		String statemarks = key.toString().trim();
		String details[] = statemarks.split(":");
		String state = details[0];
		if(state.equals("Orissa"))
			return 0;
		else if(state.equals("Jharkhand"))
			return 1;
		else if(state.equals("Andhra Pradesh"))
			return 2;
		else if(state.equals("Maharashtra"))
			return 3;
		else if(state.equals("Gujrat"))
			return 4;
		else if(state.equals("Kerala"))
			return 5;
		else if(state.equals("Delhi"))
			return 6;
		else if(state.equals("Bombay"))
			return 7;
		else if(state.equals("Madras"))
			return 8;
		else if(state.equals("Karnataka"))
			return 9;
		else if(state.equals("MH"))
			return 10;
		else if(state.equals("Vizag"))
			return 11;
		else if(state.equals("Pune"))
			return 12;
		else if(state.equals("Lucknow"))
			return 13;
		else if(state.equals("Goa"))
			return 14;
		else if(state.equals("Bangalore"))
			return 15;
		else if(state.equals("Kochi"))
			return 16;
		else if(state.equals("Chennai"))
			return 17;
		else
			return 18;
	}

}
