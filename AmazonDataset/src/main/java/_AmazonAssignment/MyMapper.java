package _AmazonAssignment;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	private static Logger logger = Logger.getLogger(MyMapper.class);
	
	public MyMapper() {
		
		logger.info("MyMapper()");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		logger.info("MyMapper setup method");
	}
	
	
	@Override
	public void run(Context context)throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.run(context);
		logger.info("MyMapper run method");
	}
	
	@Override
	protected void map(LongWritable offset, Text value,Context context) throws IOException, InterruptedException {
		
		logger.info("MyMapper map method");
		logger.info("Key:"+offset+"\n");
		logger.info("Value:");
		if(value.toString().isEmpty())
		{
			return;
		}
		Scanner scan = new Scanner(value.toString());
		StringBuffer sb = new StringBuffer();
		String key;
		key = scan.nextLine().trim();
		while(scan.hasNext())
		{
			sb.append(scan.nextLine());
			sb.append("\n");
			
		}
		context.write(new Text(key), new Text(sb.toString()));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		logger.info("MyMapper cleanup method");
	}

}
