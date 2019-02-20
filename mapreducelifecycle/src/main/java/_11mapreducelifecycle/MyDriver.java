package _11mapreducelifecycle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {

	private static final String INPUT_PATH= "hdfs://localhost:9000/user/hduser/marks/";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/user/hduser/marks_output";
	private static boolean flag;
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		MyIOUtils.uploadInputFile(INPUT_PATH);
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"Mapreducelifecycle");
		job.setJarByClass(MyDriver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job,new Path(INPUT_PATH));
		Path output_dir = new Path(OUTPUT_PATH);
		FileOutputFormat.setOutputPath(job,output_dir);
		
		FileSystem fs = output_dir.getFileSystem(conf);
		fs.delete(output_dir,true);
		
		flag = job.waitForCompletion(true);
		
		if(flag)
		{
			MyIOUtils.readOutputFile(OUTPUT_PATH);
		}

	}

}
