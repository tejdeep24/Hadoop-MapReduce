package _24MaxTempCustomPartitionerCombiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

public class MaxTemperatureCombiner  extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{

	private static final Logger LOGGER = Logger.getLogger(MaxTemperatureReducer.class);
	private static int Max_VALUE;
	
	public MaxTemperatureCombiner() {
		
		LOGGER.info("MaxTemperatureReducer()");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LOGGER.info("MyCombiner setup method");
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		LOGGER.info("MyCombiner Run method");
		super.run(context);
	}
	
	@Override
	protected void reduce(IntWritable key,Iterable<IntWritable> value,Context context)
			throws IOException, InterruptedException {
		
		LOGGER.info("CombinerReducer(-,-,-)");
		LOGGER.info("Key"+key);
		LOGGER.info("Values");
		for(IntWritable i : value)
		{
			int temp =i.get();
			if(Max_VALUE < temp)
			{
				Max_VALUE = temp;
			}
		}
		LOGGER.info("MaxTemp:"+Max_VALUE);
		context.write(key,new IntWritable(Max_VALUE));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyCombiner Cleanup method");
	}
	
}
