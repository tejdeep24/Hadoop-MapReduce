package _24MaxTempCustomPartitionerCombiner;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MaxTemperatureMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable> {
	
	private static final Logger LOGGER = Logger.getLogger(MaxTemperatureMapper.class);
	private static int year;
	private static int temp;
	
	public MaxTemperatureMapper() {
		
		LOGGER.info("MaxTemperature()");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMapper setup method");
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		LOGGER.info("MyMapper run method");
		super.run(context);
	}
	
	@Override
	protected void map(LongWritable offset, Text value,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMap(-,-,-)");
		String currentline = value.toString();
		String details[] = currentline.split(" ");
		year = Integer.parseInt(details[0]);
		temp = Integer.parseInt(details[1]);
		LOGGER.info("Mapper Output"+year+":::"+temp);
		context.write(new IntWritable(year),new IntWritable(temp));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMapper Clean up method");
	}
	

}
