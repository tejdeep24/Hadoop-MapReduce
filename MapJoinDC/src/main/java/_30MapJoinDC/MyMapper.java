package _30MapJoinDC;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	private static final Logger LOGGER = Logger.getLogger(MyMapper.class);
	private static int EmployeeId;
	private static int Salary;
	private static String FirstName;
	private static String Gender;
	private static String DeptName;
	private static Map<Integer,List<String>> empmap;
	private static Map<String,String> deptmap;
	private static List<String> emplist;
	private static Scanner scan;
	
	public MyMapper() {
		
		LOGGER.info("MyMapper");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMapper setup method");
		LOGGER.info("Loading cache file into local system");
		empmap = new HashMap<Integer,List<String>>();
		deptmap = new HashMap<String,String>();
		Path files[] = context.getLocalCacheFiles();
		for(Path file: files)
		{
			String filename = file.toString();
			LOGGER.info(filename);
			scan = new Scanner(new FileReader(filename));
			if(filename.contains("employee.txt"))
			{
				LOGGER.info("Employee details Loading");
				while(scan.hasNext())
				{
					String currentline = scan.nextLine().trim();
					if(currentline.contains("#"))
						continue;
					String details[] = currentline.split(",");
					LOGGER.info(details.length);
					LOGGER.info(details[2]+","+details[4]+","+details[6]);
					emplist = new ArrayList<String>();
					emplist.add(details[2]);
					emplist.add(details[4]);
					emplist.add(details[6]);
					empmap.put(Integer.parseInt(details[0]),emplist);
				}
				LOGGER.info("Employee details Loaded");
				LOGGER.info(empmap);
			}
			else if(filename.contains("department.txt"))
			{
				LOGGER.info("Department details Loading");
				while(scan.hasNext())
				{
					String currentline = scan.nextLine().trim();
					if(currentline.contains("#"))
						continue;
					String details[] = currentline.split(",");
					LOGGER.info(details.length);
					LOGGER.info(details[0]+","+details[1]);
					deptmap.put(details[0],details[1]);
				}
				LOGGER.info("Department details Loaded");
				LOGGER.info(deptmap);
			}
			else
				LOGGER.info("Invalid Filename");
			scan.close();
		}
		
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		LOGGER.info("MyMapper run method");
		super.run(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		LOGGER.info("Map(-,-,-)");
		LOGGER.info(key+":::"+value);
		String Currentline = value.toString().trim();
		if(Currentline.contains("#"))
			return;
		String details[] = Currentline.split(",");
		EmployeeId = Integer.parseInt(details[0]);
		Salary = Integer.parseInt(details[1]);
		List<String> list = empmap.get(EmployeeId);
		LOGGER.info(empmap.get(EmployeeId));
		Iterator<String> it = list.iterator();
		String deptid = null;
		while(it.hasNext())
		{
			FirstName = it.next();
			Gender = it.next();
			deptid = it.next();
		}
		DeptName = deptmap.get(deptid);
		String empdetails = EmployeeId+","+FirstName+","+Gender+","+DeptName;
		context.write(new Text(empdetails),new IntWritable(Salary));
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		LOGGER.info("MyMapper Cleanup method");
	}
}
