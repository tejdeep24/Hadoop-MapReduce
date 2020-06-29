package _38SecondaySort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

public class MaxTemperaturePartitioner extends Partitioner<Text,Text>{

	private static final Logger LOGGER = Logger.getLogger(MaxTemperaturePartitioner.class);
	
	public MaxTemperaturePartitioner() {
		LOGGER.info("MaxTemperaturePartitioner()");
	}
	
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		
		LOGGER.info(key+":::"+value);
		String currentline = key.toString().trim();
		String yeartemp[] = currentline.split(" ");
		int Year = Integer.parseInt(yeartemp[0]);
		return Year%numPartitions;
	}

}
