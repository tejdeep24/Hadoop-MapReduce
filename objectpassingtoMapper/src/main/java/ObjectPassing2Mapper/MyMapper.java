package ObjectPassing2Mapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MyMapper extends Mapper<LongWritable,Text,IntWritable,Student> {
	
	private static final Logger LOGGER = Logger.getLogger(MyMapper.class);
	private int Marks;
	private String StudentName;
	private String SchoolName;
	private String CityName;
	private String State;
	private Student s;
	public MyMapper() {
		
		LOGGER.info("MyMapper():"+hashCode());
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LOGGER.info("MyMapper setup method");
		Configuration conf = context.getConfiguration();
		State = conf.get("StateName");
		conf.set("FromMapper","Hello");
		String StuString = conf.get("StuObj");
		DefaultStringifier<Student> str = new DefaultStringifier<Student>(conf,Student.class);
		s = (Student)str.fromString(StuString);
		LOGGER.info("Mapper Student object hashcode"+s.hashCode());
	}
	
	@Override
	public void run(Mapper<LongWritable, Text, IntWritable,Student>.Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMapper Run method");
		super.run(context);
	}
	
	@Override
	protected void map(LongWritable offset, Text line,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMap(-,-,-)");
		LOGGER.info(offset+":::"+line);
		
		String currentline = line.toString().trim();
		
		if(currentline.isEmpty())
			return;
		String studentdetails[] = currentline.split(",");
		if(studentdetails.length !=5)
			return;
		if(!(studentdetails[3].equals(State)))
			return;
		StudentName = studentdetails[0];
		Marks = Integer.parseInt(studentdetails[1]);
		SchoolName = studentdetails[2];
		CityName = studentdetails[3];
		
		String value = StudentName+","+SchoolName+","+CityName;
		s = new Student(new Text(StudentName),new Text(SchoolName),new Text(CityName));
		
		
		context.write(new IntWritable(Marks),s);
		
		LOGGER.info("Mapoutput:"+Marks+"::::"+value);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMapper Clean up method");
	}
	
	
	

}
