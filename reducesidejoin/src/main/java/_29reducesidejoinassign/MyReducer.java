package _29reducesidejoinassign;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MyReducer extends Reducer<IntWritable,Text,Text,Text>{
	
	private static final Logger LOGGER = Logger.getLogger(MyReducer.class);
	private static String CustomerName;
	private static int amount;
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
	protected void reduce(IntWritable key, Iterable<Text> value,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Reduce(-,-,-)");
		amount =0;
		int count =0;
		
		for(Text t : value)
		{
			String d = t.toString();
			String details[] = d.split(",");
			if(details[0].equals("Cus"))
			{
				CustomerName = details[1];
			}
			else if(details[0].equals("Trans"))
			{
				count++;
				amount += Integer.parseInt(details[2]);
			}
			else
				return;
		}
		String detail = count+","+amount;
		context.write(new Text(CustomerName),new Text(detail));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer clean up method");
	}

}
