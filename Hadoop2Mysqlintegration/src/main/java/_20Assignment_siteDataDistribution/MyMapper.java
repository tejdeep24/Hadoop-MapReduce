package _20Assignment_siteDataDistribution;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sun.istack.logging.Logger;

public class MyMapper extends Mapper<LongWritable,Text,IntWritable,Student> {
	
	private static final Logger LOGGER = Logger.getLogger(MyMapper.class);
	private static String state;
	private String StudentName;
	private String SchoolName;
	private String CityName;
	private int Marks;
	private Connection c;
	private PreparedStatement ps;
	private static String driverName;
	private static String url;
	private static String user;
	private static String password;
	private ArrayList<String> list = new ArrayList<String>();
	
	public MyMapper() {
		
		LOGGER.info("MyMapper():"+hashCode());
	}
	
	@SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMapper setup Method");
		state = context.getConfiguration().get("State");
		Path files[] = context.getLocalCacheFiles();
		for (int i = 0; i < files.length; i++)
		{
			String p = files[i].toString();
			LOGGER.info("hii");
			LOGGER.info(p);
			String filename = files[i].getName();
			LOGGER.info("Hello");
			LOGGER.info(filename);
			Scanner scan = new Scanner(new FileReader(p));
			String pr;
			while(scan.hasNext())
			{
				pr = scan.nextLine().trim();
				String prop[] = pr.split("=");
				for(String s:prop)
				{
					list.add(s);
				}
			}
			scan.close();
		}
		Iterator<String> it = list.iterator();
		while(it.hasNext())
		{
			it.next();
			driverName = it.next().trim();
			LOGGER.info(driverName);
			it.next();
			url = it.next().trim();
			LOGGER.info(url);
			it.next();
			user= it.next().trim();
			LOGGER.info(user);
			it.next();
			password = it.next().trim();
			LOGGER.info(password);
		}
		try
		{
			LOGGER.info("Class Name");
			Class.forName("com.mysql.jdbc.Driver");
			LOGGER.info("Class Name");
			c = DriverManager.getConnection("jdbc:mysql://localhost/test","root","root");
		}
		catch(Exception e)
		{
			LOGGER.info("Exception:"+e.getStackTrace());
			LOGGER.info("Connection is not successfull");
		}
		
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		LOGGER.info("MyMapper run Method");
		super.run(context);
	}
	
	@Override
	protected void map(LongWritable Offset, Text line,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMap(-,-,-)");
		LOGGER.info(Offset+":::"+line);
		String currentline = line.toString().trim();
		String studentdetails[] = currentline.split(",");
		
		if(studentdetails.length < 5)
			return;
		if(state.equals(studentdetails[3]))
		{
			StudentName = studentdetails[0];
			Marks = Integer.parseInt(studentdetails[1]);
			SchoolName = studentdetails[2];
			CityName = studentdetails[3];
			String value = StudentName+","+SchoolName+","+CityName;
			Student st = new Student(new Text(StudentName),new Text(SchoolName),new Text(CityName));
			LOGGER.info("MyMapper Output"+Marks+":::"+value);
			context.write(new IntWritable(Marks),st);
		}
		else{
			try
			{
				ps = c.prepareStatement("INSERT INTO student VALUES(?,?,?,?,?)");
				ps.setString(1,studentdetails[0]);
				ps.setInt(2,Integer.parseInt(studentdetails[1]));
				ps.setString(3,studentdetails[2]);
				ps.setString(4,studentdetails[3]);
				ps.setString(5,studentdetails[4]);
				int  i = ps.executeUpdate();
				LOGGER.info("Update"+i);
			}
			catch(Exception e)
			{
				LOGGER.info("Exception:"+e.getStackTrace());
				LOGGER.info("Error in inserting the data");
			}
			
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMapper Clean up Method");
	}
	

}
