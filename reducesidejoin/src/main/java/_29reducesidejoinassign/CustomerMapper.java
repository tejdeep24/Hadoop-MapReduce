package _29reducesidejoinassign;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class CustomerMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
	
	private static final Logger LOGGER = Logger.getLogger(CustomerMapper.class);
	private static int CustomerId;
	private static String FirstName;
	
	public CustomerMapper() {
		
		LOGGER.info("CustomerMapper()");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Customer Mapper setup method");
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		LOGGER.info("Customer Mapper run method");
		super.run(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Customer map(-,-,-)");
		LOGGER.info(key+":::"+value);
		String currentline = value.toString().trim();
		if(currentline.contains("#"))
			return;
		String details[] = currentline.split(",");
		CustomerId = Integer.parseInt(details[0]);
		FirstName = details[1];
		String customerdata = "Cus"+","+FirstName;
		LOGGER.info(CustomerId+":::"+customerdata);
		context.write(new IntWritable(CustomerId),new Text(customerdata));
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Customer Mapper Cleanup method");
	}
	

}
