package _16customwritable_withlogging;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MyReducer extends Reducer<IntWritable,Student,IntWritable,Student>{
	
	private static final Logger LOGGER = Logger.getLogger(MyReducer.class);
	private int topscore=0;
	private ArrayList<Student> list;
	
	public MyReducer() {
		
		LOGGER.info("MyReducer()"+hashCode());
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer setup method");
		list = new ArrayList<Student>();
	}
	
	@Override
	public void run(Reducer<IntWritable,Student, IntWritable,Student>.Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer run method");
		super.run(context);
	}
	
	@Override
	protected void reduce(IntWritable Marks, Iterable<Student> value,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReduce(-,-,)");
		int currentMarks = Marks.get();
		if(currentMarks > topscore)
		{
			topscore = currentMarks;
			
			list.clear();
			for(Student details: value)
			{
				list.add(details);
			}
			
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer Clean Up method");
		LOGGER.info("Highest Score Details");
		
		for(Student li: list)
		{
			context.write(new IntWritable(topscore),li);
		}
	}
	
	

}
