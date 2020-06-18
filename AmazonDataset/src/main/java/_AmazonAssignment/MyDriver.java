package _AmazonAssignment;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

public class MyDriver {
	
	private static String inputpath = "hdfs://localhost:9000/user/hduser/amzdata";
	private static String outputpath = "hdfs://localhost:9000/user/hduser/amzoutput";
	private static Logger logger = Logger.getLogger(MyDriver.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		MyIOUtils.uploadInputFile(inputpath);
		
		Configuration conf = new Configuration();
		conf.set("Starttag","Id:");
		conf.set("Endtag"," ");
		Job job = Job.getInstance(conf);
		job.setJarByClass(MyDriver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		//job.setNumReduceTasks(0);
		job.setInputFormatClass(MyNlineInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path input_path = new Path(inputpath);
		Path output_path = new Path(outputpath);
		output_path.getFileSystem(conf).delete(output_path, true);
		
		FileInputFormat.addInputPath(job,input_path);
		FileOutputFormat.setOutputPath(job,output_path);
		LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);
		
		boolean flag = job.waitForCompletion(true);
		
		if(flag)
		{
			logger.info("Job Execution completed");
			MyIOUtils.readOutputFile(outputpath);
			
		}
		else
		{
			logger.info("Job Failed !!!!!!!!!!!!");
		}

	}

}
