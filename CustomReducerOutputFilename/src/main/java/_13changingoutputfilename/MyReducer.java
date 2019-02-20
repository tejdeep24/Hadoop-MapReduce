package _13changingoutputfilename;

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
	protected void reduce(Text word, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{	
		int count =0;
		for(IntWritable value: values)
		{
			LOGGER.info(" "+value.get());
			count += value.get();
		}
		
		mos.write(word,new IntWritable(count),"Fruits_Sports.txt");
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
