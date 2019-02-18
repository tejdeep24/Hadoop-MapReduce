package _24MaxTempCustomPartitionerCombiner;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperatureDriver {
	
	private static final String INPUT_PATH = "hdfs://localhost:9000/user/hduser/temperaturedata";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/user/hduser/temperaturedata_output";
	private static boolean flag;
	

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
	
		MyIOUtils.uploadInputFiles("hdfs://localhost:9000/user/hduser/temperaturedata");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJobName("MaxTemperature");
		job.setJarByClass(MaxTemperatureDriver.class);
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setCombinerClass(MaxTemperatureCombiner.class);
		job.setNumReduceTasks(2);
		job.setPartitionerClass(MaxTemperaturePartitioner.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path input_path = new Path(INPUT_PATH);
		Path output_path = new Path(OUTPUT_PATH);
		
		FileInputFormat.addInputPath(job,input_path);
		FileOutputFormat.setOutputPath(job, output_path);
		
		output_path.getFileSystem(conf).delete(output_path, true);
		
		flag = job.waitForCompletion(true);
		
		if(flag)
		{
			MyIOUtils.readOutputFiles("hdfs://localhost:9000/user/hduser/temperaturedata_output");
		}

	}

}
