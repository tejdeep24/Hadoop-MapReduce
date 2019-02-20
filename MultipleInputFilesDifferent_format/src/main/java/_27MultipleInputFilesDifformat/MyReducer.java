package _27MultipleInputFilesDifformat;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MyReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>{
	
	private static final Logger LOGGER = Logger.getLogger(MyReducer.class);
	
	public MyReducer() {
		
		LOGGER.info("MyReducer");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer setup method");
	}
	
	@Override
	public void run(Context arg0) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer run method");
		super.run(arg0);
	}
	
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> value,Context context)
			throws IOException, InterruptedException {
		
		LOGGER.info("reduce(-,-,-)");
		LOGGER.info(key);
		LOGGER.info("All Values");
		float sum = 0;
		for(FloatWritable v : value)
		{
			float salary = v.get();
			sum +=salary;
			LOGGER.info(salary);
		}
		context.write(key,new FloatWritable(sum));
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer clean up method");
	}
	

}
