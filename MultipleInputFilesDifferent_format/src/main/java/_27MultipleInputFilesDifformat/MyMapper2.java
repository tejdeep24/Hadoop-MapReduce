package _27MultipleInputFilesDifformat;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MyMapper2 extends Mapper<LongWritable,Text,Text,FloatWritable> {
	
	private static final Logger LOGGER = Logger.getLogger(MyMapper2.class);
	
	public MyMapper2() {
		
		LOGGER.info("MyMapper2()");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMapper2 setup method");
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		LOGGER.info("MyMapper2 run method");
		super.run(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		LOGGER.info("Mapper2 Map(-,-,-)");
		LOGGER.info(key+":::"+value);
		String currentline = value.toString().trim();
		String details[] = currentline.split(",");
		String gender = details[3];
		float salaray = Float.parseFloat(details[4]);
		context.write(new Text(gender),new FloatWritable(salaray));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMapper2 cleaup method");
	}
}
