package _38SecondarySortTopScore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MyReducer extends Reducer<Text,Student,Text,Student>{
	
	private static final Logger LOGGER = Logger.getLogger(MyReducer.class);
	private static String state;
	private static List<Student> list;
	
	public MyReducer() {
		
		LOGGER.info("MyReducer()");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Reducer setup method");
		list = new ArrayList<Student>();
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Reducer run method");
		super.run(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<Student> values,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("reduce(-,-,-)");
		LOGGER.info(key+":::"+values);
		String statemarks = key.toString().trim();
		String details[] = statemarks.split(":");
		state = details[0];
		for(Student st: values)
		{
			list.add(new Student(st));
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Reducer cleanup method");
		LOGGER.info(state);
		for(Student st:list)
		{
			context.write(new Text(state),st);
		}
	}
}
