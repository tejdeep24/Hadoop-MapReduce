package _11mapreducelifecycle;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sun.istack.logging.Logger;

public class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	private static final Logger LOGGER = Logger.getLogger(MyMapper.class);
	
	public MyMapper() {
		
		LOGGER.info("MyMapper():"+hashCode());
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LOGGER.info("MyMapper Setup method");
	}
	
	@Override
	public void run(
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		LOGGER.info("MyMapper run method");
		super.run(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMap(-,-,-)");
		LOGGER.info("Key:"+key+"value:"+value+"Context:"+context);
		
		String currentline = value.toString();
		String words[] = currentline.split(" ");
		
		StackTraceElement[] stackTraceElements = Thread.currentThread()
				.getStackTrace();

		for (StackTraceElement element : stackTraceElements)
			System.out.println(element);
		
		for(String word: words)
		{
			context.write(new Text(word),new IntWritable(1));
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMapper cleanup method");
	}
	
}
