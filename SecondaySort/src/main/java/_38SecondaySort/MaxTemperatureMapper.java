package _38SecondaySort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MaxTemperatureMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	private static final Logger LOGGER = Logger.getLogger(MaxTemperatureMapper.class);
	private int Year;
	private int temp;
	private String city;
	
	public MaxTemperatureMapper() {
		
		LOGGER.info("MaxTemperature()");
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
		String Yeartempdetails[]= currentline.split(" ");
		
		Year = Integer.parseInt(Yeartempdetails[0]);
		String tempcity = Yeartempdetails[1];
		String tempcitydetails[] = tempcity.split(":");
		
		temp = Integer.parseInt(tempcitydetails[0]);
		city = tempcitydetails[1];
		
		LOGGER.info(Year+"::"+temp+"::"+city);
		
		String keyvalue = Year+" "+temp;
		
		context.write(new Text(keyvalue),value);
		
		
	}
	
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Mapper clean up method");
	}
}
