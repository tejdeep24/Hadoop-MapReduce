package _37PrimaryReverseSorting;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class MaxTemperatureDriver {
	
	private static final Logger LOGGER = Logger.getLogger(MaxTemperatureDriver.class);
	private static final String INPUT_PATH = "hdfs://localhost:9000/user/hduser/reversesort";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/user/hduser/reversesort_output";
	private static boolean flag;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		MyIOUtils.uploadInputFiles(INPUT_PATH);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("PrimaryReverseSort");
		job.setJarByClass(MaxTemperatureDriver.class);
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setSortComparatorClass(MyKeyComparator.class);
		
		Path input_dir = new Path(INPUT_PATH);
		Path output_dir = new Path(OUTPUT_PATH);
		
		FileInputFormat.addInputPath(job,input_dir);
		FileOutputFormat.setOutputPath(job,output_dir);
		output_dir.getFileSystem(conf).delete(output_dir,true);
		
		flag = job.waitForCompletion(true);
		
		if(flag)
		{
			MyIOUtils.readOutputFiles(OUTPUT_PATH);
			LOGGER.info("job Executed successfully");
		}
		
	}

}
