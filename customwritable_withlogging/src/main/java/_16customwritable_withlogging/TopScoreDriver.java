package _16customwritable_withlogging;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopScoreDriver {
	
	private static final String INPUT_PATH = "hdfs://localhost:9000/user/hduser/studentdata";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/user/hduser/studentdata_output";
	private static boolean flag;
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		MyIOUtils.uploadInputFiles(INPUT_PATH);
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("TopScoreDriver");
		
		job.setJarByClass(TopScoreDriver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Student.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Student.class);
		
		Path input_path = new Path(INPUT_PATH);
		FileInputFormat.addInputPath(job,input_path);
		
		Path output_path = new Path(OUTPUT_PATH);
		FileOutputFormat.setOutputPath(job,output_path);
		
		FileSystem fs = output_path.getFileSystem(conf);
		
		fs.delete(output_path, true);
		
		flag = job.waitForCompletion(true);
		
		if(flag)
		{
			MyIOUtils.readOutputFiles(OUTPUT_PATH);
		}

	}

}
