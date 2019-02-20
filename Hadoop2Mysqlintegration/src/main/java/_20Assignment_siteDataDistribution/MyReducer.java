package _20Assignment_siteDataDistribution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

public class MyReducer extends Reducer<IntWritable,Student,IntWritable,Student> {
	
	private static final Logger LOGGER = Logger.getLogger(MyReducer.class);
	private int topscore=0;
	private ArrayList<Student> list = null;
	private TreeMap<IntWritable,ArrayList<Student>> hm = null;
	private static MultipleOutputs<IntWritable,Student> mos= null;
	private static String pos;
	private static int value;
	public MyReducer() {
		
		LOGGER.info("MyReducer():"+hashCode());
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer Setup method");
		pos = context.getConfiguration().get("Positon");
		value = Integer.parseInt(pos);
		hm = new TreeMap<IntWritable,ArrayList<Student>>();
		mos = new MultipleOutputs<IntWritable,Student>(context);
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer Run Method");
		super.run(context);
	}
	
	@Override
	protected void reduce(IntWritable Marks, Iterable<Student> details,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer(-,-,-)");	
		LOGGER.info(Marks+":::"+details);
		int marks = Marks.get();
		list = new ArrayList<Student>();
		list.clear();
		for(Student d: details)
		{
			list.add(new Student(d));
		}
		hm.put(new IntWritable(marks), list);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyReducer cleanup method");
		LOGGER.info("second Top scorer");
		NavigableSet<IntWritable> nm = hm.descendingKeySet();
		Iterator<IntWritable> it = nm.iterator();
		int count =0;
		while(it.hasNext())
		{
			count++;
			if(value == count)
			{
				topscore = it.next().get();
				list =hm.get(new IntWritable(topscore));
			}
			it.next();
		}
		
		for(Student s:list )
		{
			mos.write(new IntWritable(topscore),s,"TopScore.txt");
		}
		mos.close();
	}
	
	

}
