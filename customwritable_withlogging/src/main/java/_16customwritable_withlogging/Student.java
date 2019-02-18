package _16customwritable_withlogging;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class Student implements Writable {

	private static final Logger LOGGER = Logger.getLogger(Student.class);
	private Text StudentName;
	private Text SchoolName;
	private Text CityName;
	
	public Student()
	{
		LOGGER.info("default constructor");
		this.StudentName = new Text();
		this.SchoolName = new Text();
		this.CityName = new Text();
	}
	
	public Student(Text StudentName,Text SchoolName,Text CityName) {
		
		LOGGER.info("Parameter Constructor");
		this.StudentName = StudentName;
		this.SchoolName = SchoolName;
		this.CityName = CityName;
	}
	
	public Student(Student st)
	{
		LOGGER.info("deep Copy Constructor");
		StudentName = new Text(st.StudentName);
		SchoolName = new Text(st.SchoolName);
		CityName = new Text(st.CityName);
	}
	
	public String toString()
	{
		LOGGER.info("ToString Method");
		return StudentName+","+SchoolName+","+CityName;
	}
	public void readFields(DataInput in) throws IOException {
		
		LOGGER.info("Deserialization of object");
		StudentName.readFields(in);
		SchoolName.readFields(in);
		CityName.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		
		LOGGER.info("Serialization of Object");
		StudentName.write(out);
		SchoolName.write(out);
		CityName.write(out);
	}

}
