package _38SecondarySortTopScore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class Student implements Writable{
	
	private Text StudentName;
	private IntWritable Marks;
	private Text SchoolName;
	private Text State;
	private Text StudentMailId;
	private static final Logger LOGGER = Logger.getLogger(Student.class);
	
	public Student()
	{
		LOGGER.info("Default constructor");
		this.StudentName = new Text();
		this.Marks = new IntWritable();
		this.SchoolName = new Text();
		this.State = new Text();
		this.StudentMailId = new Text();
	}
	public Student(Text StudentName,IntWritable Marks,Text SchoolName,Text State,Text StudentMailId) {
		
		LOGGER.info("Parameterized Constructor()");
		this.StudentName = StudentName;
		this.Marks = Marks;
		this.SchoolName = SchoolName;
		this.State = State;
		this.StudentMailId = StudentMailId;
	}
	public Student(Student st)
	{
		LOGGER.info("Deep Copy Constructor");
		this.StudentName = new Text(st.StudentName);
		this.Marks = new IntWritable(st.Marks.get());
		this.SchoolName = new Text(st.SchoolName);
		this.State = new Text(st.State);
		this.StudentMailId = new Text(st.StudentMailId);
	}
	public void readFields(DataInput in) throws IOException {
		
		LOGGER.info("Deserialization");
		StudentName.readFields(in);
		Marks.readFields(in);
		SchoolName.readFields(in);
		State.readFields(in);
		StudentMailId.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		
		LOGGER.info("Serialization()");
		StudentName.write(out);
		Marks.write(out);
		SchoolName.write(out);
		State.write(out);
		StudentMailId.write(out);
	}
	
	public String toString()
	{
		return StudentName+","+Marks+","+SchoolName+","+State+","+StudentMailId;
	}

}
