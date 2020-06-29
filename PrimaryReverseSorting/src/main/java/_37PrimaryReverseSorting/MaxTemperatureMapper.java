package _37PrimaryReverseSorting;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MaxTemperatureMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
	
	private static final Logger LOGGER = Logger.getLogger(MaxTemperatureMapper.class);
	public MaxTemperatureMapper() {
		
		LOGGER.info("MaxTemperatureMapper()");
	}
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Mapper setup method");
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		LOGGER.info("Mapper run method");
		super.run(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Map(-,-,-)");
		LOGGER.info(key+":::"+value);
		String currentline = value.toString().trim();
		String tempdetails[] = currentline.split(" ");
		int Year = Integer.parseInt(tempdetails[0]);
		int temp = Integer.parseInt(tempdetails[1]);
		LOGGER.info(Year+":::"+temp);
		context.write(new IntWritable(Year),new IntWritable(temp));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Mapper cleanup method");
	}
	
	


}
