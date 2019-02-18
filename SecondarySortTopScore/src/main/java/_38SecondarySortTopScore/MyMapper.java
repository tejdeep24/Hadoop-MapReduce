package _38SecondarySortTopScore;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MyMapper extends Mapper<LongWritable,Text,Text,Student>{
	
	private static final Logger LOGGER = Logger.getLogger(MyMapper.class);
	private String StudentName;
	private int Marks;
	private String SchoolName;
	private String State;
	private String StudentMailId;
	
	public MyMapper() {
		LOGGER.info("MyMapper()");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Mapper setup method");
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Mapper run method");
		super.run(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("map(-,-,-)");
		LOGGER.info(key+":::"+value);
		String currentline = value.toString().trim();
		String details[] = currentline.split(",");
		StudentName = details[0];
		Marks = Integer.parseInt(details[1]);
		SchoolName = details[2];
		State = details[3];
		StudentMailId = details[4];
		String mapkey = State+":"+Marks;
		String mapvalue = StudentName+","+Marks+","+SchoolName+","+StudentMailId;
		Student st = new Student(new Text(StudentName),new IntWritable(Marks),new Text(SchoolName),new Text(State),new Text(StudentMailId));
		LOGGER.info(mapkey+":::"+mapvalue);
		context.write(new Text(mapkey),st);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Mapper cleanup method");
	}
	
	

}
