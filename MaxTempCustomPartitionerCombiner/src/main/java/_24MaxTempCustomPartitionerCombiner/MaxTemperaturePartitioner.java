package _24MaxTempCustomPartitionerCombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

public class MaxTemperaturePartitioner extends Partitioner<IntWritable,IntWritable> {
	
	private static final Logger LOGGER = Logger.getLogger(MaxTemperaturePartitioner.class);
	
	public MaxTemperaturePartitioner() {
		
		LOGGER.info("MaxTemperaturePartitioner()");
	}

	@Override
	public int getPartition(IntWritable key, IntWritable value,int numPartitions) {
		// TODO Auto-generated method stub
		LOGGER.info("GetPartition");
		int year = key.get();
		if(year % 2 ==0)
			return 0;
		else
			return 1;
	}
	
	

}
