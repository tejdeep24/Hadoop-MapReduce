package _37PrimaryReverseSorting;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MaxTemperatureReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
	
	private static final Logger LOGGER = Logger.getLogger(MaxTemperatureReducer.class);
	
	public MaxTemperatureReducer() {
		
		LOGGER.info("MaxTemperatureReader()");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	
			LOGGER.info("Reducer setup method");
	}
	
	@Override
	public void run(Context arg0) throws IOException, InterruptedException {
		
		LOGGER.info("Reducer run metod");
		super.run(arg0);
	}
	
	@Override
	protected void reduce(IntWritable Year,Iterable<IntWritable> temps,Context context)
			throws IOException, InterruptedException {
		
		LOGGER.info("Reduce(-,-,-)");
		LOGGER.info(Year);
		LOGGER.info("All Values");
		int Maxtemp =0;
		int temp;
		for(IntWritable t: temps)
		{
			temp = t.get();
			LOGGER.info(temp);
			if(temp > Maxtemp)
			{
				Maxtemp = temp;
			}
		}
		LOGGER.info(Year+":::"+Maxtemp);
		context.write(Year, new IntWritable(Maxtemp));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Reducer cleanup method");
	}
	

}
