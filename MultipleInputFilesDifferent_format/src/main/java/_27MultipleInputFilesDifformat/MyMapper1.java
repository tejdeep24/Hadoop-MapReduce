package _27MultipleInputFilesDifformat;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MyMapper1 extends Mapper<LongWritable,Text,Text,FloatWritable> {
	
	private static final Logger LOGGER = Logger.getLogger(MyMapper1.class);
	
	public MyMapper1() {
		
		LOGGER.info("MyMapper1()");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMapper1 setup method");
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		LOGGER.info("MyMapper1 run method");
		super.run(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		LOGGER.info("Mapper1 Map(-,-,-)");
		LOGGER.info(key+":::"+value);
		String currentline = value.toString().trim();
		String details[] = currentline.split(",");
		String gender = details[1];
		float salaray = Float.parseFloat(details[2]);
		context.write(new Text(gender),new FloatWritable(salaray));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMapper1 cleaup method");
	}
	

}
