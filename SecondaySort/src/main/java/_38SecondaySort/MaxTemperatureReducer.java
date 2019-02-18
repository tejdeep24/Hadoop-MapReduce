package _38SecondaySort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MaxTemperatureReducer extends Reducer<Text,Text,Text,Text>{
	
	private static final Logger LOGGER = Logger.getLogger(MaxTemperatureReducer.class);
	
	public MaxTemperatureReducer() {
		
		LOGGER.info("MaxTemperatureReducer()");
	}
	
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		
		LOGGER.info("Reducer setup method");
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Reducer run method");
		super.run(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException {
		
		LOGGER.info("Reduce(-,-,-)");
		LOGGER.info(key);
		LOGGER.info("All values");
		
		for(Text value:values)
		{
			LOGGER.info(value);
			context.write(key,value);
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Reducer Cleanup method");
	}
	

}
