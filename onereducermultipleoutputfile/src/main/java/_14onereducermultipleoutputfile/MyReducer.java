package _14onereducermultipleoutputfile;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

public class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	private static final Logger LOGGER = Logger.getLogger(MyReducer.class);
	private MultipleOutputs<Text,IntWritable> mos = null;
	
	public MyReducer() {
		
		LOGGER.info("MyReducer():"+hashCode());
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer setup method");
		mos = new MultipleOutputs<Text, IntWritable>(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{	
		int count =0;
		for(IntWritable value: values)
		{
			LOGGER.info(" "+value.get());
			count += value.get();
		}
		
		String word = key.toString();
		if(word.startsWith("s_"))
		{
			String w = word.substring(2);
			mos.write(new Text(w),new IntWritable(count),"Sports.txt");
		}
		else if(word.startsWith("f_"))
		{
			String w = word.substring(2);
			mos.write(new Text(w),new IntWritable(count),"Fruits.txt");
		}
		else
		{
			context.write(new Text(word),new IntWritable(count));
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer cleanup method");
		mos.close();
	}
	
	@Override
	public void run(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer run method");
		super.run(context);
	}
	

}
