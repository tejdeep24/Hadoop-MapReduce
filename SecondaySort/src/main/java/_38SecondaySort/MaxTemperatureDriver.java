package _38SecondaySort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class MaxTemperatureDriver {
	
	private static final Logger LOGGER = Logger.getLogger(MaxTemperatureDriver.class);
	private static final String INPUT_PATH = "hdfs://localhost:9000/user/hduser/SecondarySort";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/user/hduser/SecondarySort_Output";
	private static boolean flag;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
			MyIOUtils.uploadInputFiles(INPUT_PATH);
			
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			job.setJobName("SecondarySort");
			job.setJarByClass(MaxTemperatureDriver.class);
			job.setMapperClass(MaxTemperatureMapper.class);
			job.setReducerClass(MaxTemperatureReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setSortComparatorClass(MyKeyComparator.class);
			job.setPartitionerClass(MaxTemperaturePartitioner.class);
			job.setGroupingComparatorClass(MyGroupComparator.class);
			job.setNumReduceTasks(2);
			Path input_dir = new Path(INPUT_PATH);
			Path output_dir = new Path(OUTPUT_PATH);
			
			FileInputFormat.addInputPath(job,input_dir);
			FileOutputFormat.setOutputPath(job,output_dir);
			
			
			
			output_dir.getFileSystem(conf).delete(output_dir, true);
			
			flag = job.waitForCompletion(true);
			
			if(flag)
			{
				MyIOUtils.readOutputFiles(OUTPUT_PATH);
				LOGGER.info("job Executed successfully");
			}

	}

}
