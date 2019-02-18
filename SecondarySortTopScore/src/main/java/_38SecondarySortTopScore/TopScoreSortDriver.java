package _38SecondarySortTopScore;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class TopScoreSortDriver {
	
	private static final Logger LOGGER = Logger.getLogger(TopScoreSortDriver.class);
	private static final String INPUT_PATH = "hdfs://localhost:9000/user/hduser/studentdata";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/user/hduser/studentdata_output";
	private static boolean flag;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		MyIOUtils.uploadInputFiles(INPUT_PATH);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("SecondarySortTopScore");
		job.setJarByClass(TopScoreSortDriver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Student.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Student.class);
		job.setNumReduceTasks(20);
		job.setPartitionerClass(MyPartitioner.class);
		job.setSortComparatorClass(MyKeyComparator.class);
		job.setGroupingComparatorClass(MyGroupComparator.class);
		Path input_dir = new Path(INPUT_PATH);
		Path output_dir = new Path(OUTPUT_PATH);
		
		FileInputFormat.addInputPath(job,input_dir);
		FileOutputFormat.setOutputPath(job,output_dir);
		
		output_dir.getFileSystem(conf).delete(output_dir, true);
		
		flag = job.waitForCompletion(true);
		
		if(flag)
		{
			MyIOUtils.readOutputFiles(OUTPUT_PATH);
			LOGGER.info("Job Executed successfully");
		}
	}

}
