package _11mapreducelifecycle;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	private static final Logger LOGGER = Logger.getLogger(MyReducer.class);
	
	
	public MyReducer() {
		
		LOGGER.info("MyReducer():"+hashCode());
	}
	
	@Override
	protected void setup(Context context)
	{
			LOGGER.info("MyReducer setup method");
	}
	
	@Override
	public void run(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		LOGGER.info("MyReducer run method");
		super.run(context);
	}
	
	@Override
	protected void reduce(Text word, Iterable<IntWritable> value,Context context)
			throws IOException, InterruptedException {
		
		LOGGER.info("reduce(-,-,-)");
		LOGGER.info("Context:"+context);
		LOGGER.info("Key:"+word);
		int count =0;
		Iterator<IntWritable> it = value.iterator();
		
		StackTraceElement[] stackTraceElements = Thread.currentThread()
				.getStackTrace();

		for (StackTraceElement element : stackTraceElements)
			System.out.println(element);
		
		while(it.hasNext())
		{
			it.next();
			count++;
		}
		context.write(word,new IntWritable(count));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducere cleanup method");
	}
	

}
