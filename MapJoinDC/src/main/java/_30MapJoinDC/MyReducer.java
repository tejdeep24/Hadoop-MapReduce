package _30MapJoinDC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	private static final Logger LOGGER = Logger.getLogger(MyReducer.class);
	private static int Total_amount;

	public MyReducer() {
		
		LOGGER.info("MyReducer()");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer setup method");
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer run method");
		super.run(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> salaries,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Reduce(-,-,-)");
		LOGGER.info(key+":::"+salaries);
		Total_amount =0;
		for(IntWritable salary : salaries)
		{
			Total_amount += salary.get();	
		}
		context.write(key,new IntWritable(Total_amount));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer cleanup method");
	}
}
