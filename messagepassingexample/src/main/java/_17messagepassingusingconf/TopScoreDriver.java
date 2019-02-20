package _17messagepassingusingconf;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

@SuppressWarnings("unused")
public class TopScoreDriver {
	
	private static final String INPUT_PATH = "hdfs://localhost:9000/user/hduser/studentdata";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/user/hduser/studentdata_output";
	private static boolean flag;
	private static final Logger LOGGER = Logger.getLogger(TopScoreDriver.class);
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		MyIOUtils.uploadInputFiles(INPUT_PATH);
		
		String state = getStateFromUser();
		LOGGER.info("Processing for State");
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("TopScoreDriver");
		
		job.setJarByClass(TopScoreDriver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.getConfiguration().set("StateName",state);
		
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Student.class);
		
		Path input_path = new Path(INPUT_PATH);
		FileInputFormat.addInputPath(job,input_path);
		
		Path output_path = new Path(OUTPUT_PATH);
		FileOutputFormat.setOutputPath(job,output_path);
		
		output_path.getFileSystem(job.getConfiguration()).delete(output_path,true);
		
		flag = job.waitForCompletion(true);
		
		if(flag)
		{
			MyIOUtils.readOutputFiles(OUTPUT_PATH);
		}

	}
	
	@SuppressWarnings("resource")
	private static String getStateFromUser()
	{
		Scanner sc = new Scanner(System.in);
		LOGGER.info("Enter the state Name");
		String state = sc.next();
		return state;
		
	}

}
