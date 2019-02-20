package _20Assignment_siteDataDistribution;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class DataDistributionDriver {
	
	private static final String INPUT_PATH = "hdfs://localhost:9000/user/hduser/studentdata/";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/user/hduser/studentdetails";
	private static boolean flag;
	private static final Logger LOGGER = Logger.getLogger(DataDistributionDriver.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		//MyIOUtils.uploadInputFiles(INPUT_PATH);
		String state = getState();
		String position = getPosition();
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.getConfiguration().set("State",state);
		job.getConfiguration().set("Positon",position);
		job.setJobName("DataDistributionDriver");
		job.setJarByClass(DataDistributionDriver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.addCacheFile(new URI("/myapp/mydriver.properties"));
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Student.class);
		
		Path input_dir = new Path(INPUT_PATH);
		Path output_dir = new Path(OUTPUT_PATH);
		
		FileInputFormat.addInputPath(job,input_dir);
		FileOutputFormat.setOutputPath(job,output_dir);
		
		output_dir.getFileSystem(conf).delete(output_dir, true);
		
		flag = job.waitForCompletion(true);
		
		/*if(flag)
		{
			MyIOUtils.readOutputFiles(OUTPUT_PATH);
		}*/

	}
	
	public static String getState()
	{
		Scanner sc = new Scanner(System.in);
		LOGGER.info("Enter the state to be processed");
		String state = sc.next();
		return state;
	}
	
	public static String getPosition()
	{
		Scanner sc = new Scanner(System.in);
		LOGGER.info("Enter the Position to display");
		String position  = sc.next();
		return position;
	}

}
