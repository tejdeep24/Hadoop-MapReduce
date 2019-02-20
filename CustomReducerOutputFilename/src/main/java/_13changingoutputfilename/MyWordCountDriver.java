package _13changingoutputfilename;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyWordCountDriver {
	
	private static final String INPUT_PATH = "hdfs://localhost:9000/user/hduser/marks";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/user/hduser/marks_output";
	private static boolean flag;
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		String input = MyIOUtils.uploadInputFiles(INPUT_PATH);
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("MyWordCount.java");
		
		job.setJarByClass(MyWordCountDriver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		 
		Path output_dir = new Path(OUTPUT_PATH);
		
		FileInputFormat.addInputPath(job,new Path(input));
		FileOutputFormat.setOutputPath(job, output_dir);
		
		FileSystem fs = output_dir.getFileSystem(conf);
		
		fs.delete(output_dir,true);
		
		flag = job.waitForCompletion(true);
		
		if(flag)
		{
			MyIOUtils.readOutputFiles(OUTPUT_PATH);
		}
		
		

	}

}
