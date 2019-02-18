package _29reducesidejoinassign;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class TransactionMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
	
	private static final Logger LOGGER = Logger.getLogger(TransactionMapper.class);
	private static int TransactionId;
	private static int CustomerId;
	private static int amount;
	private static int Quantity;
	private static int Total;

	public TransactionMapper() {
		
		LOGGER.info("TransactionMapper()");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Transaction Mapper setup method");
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Transaction Mapper run method");
		super.run(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Transaction map(-,-,-)");
		LOGGER.info(key+":::"+value);
		String currentline = value.toString().trim();
		if(currentline.contains("#"))
			return;
		String details[] = currentline.split(",");
		CustomerId = Integer.parseInt(details[2]);
		amount = Integer.parseInt(details[5]);
		Quantity = Integer.parseInt(details[6]);
		Total = amount * Quantity;
		TransactionId = Integer.parseInt(details[0]);
		String trans = "Trans"+","+TransactionId+","+Total;
		LOGGER.info(CustomerId+":::"+trans);
		context.write(new IntWritable(CustomerId),new Text(trans));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Transaction Mapper cleanup method");
	}
	
}
