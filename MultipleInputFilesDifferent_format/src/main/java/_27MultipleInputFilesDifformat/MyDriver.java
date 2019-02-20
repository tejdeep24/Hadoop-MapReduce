package _27MultipleInputFilesDifformat;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;


public class MyDriver {
	
	private static final Logger LOGGER = Logger.getLogger(MyDriver.class);
	private static final String INPUT_PATH = "hdfs://localhost:9000/user/hduser/emp_data";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/user/hduser/empdata_output";
	private static boolean flag;
	private static String filename;
	private static String url = "hdfs://localhost:9000";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		MyIOUtils.uploadInputFiles(INPUT_PATH);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("MultipleInputFormatFiles");
		job.setJarByClass(MyDriver.class);
		job.setMapperClass(MyMapper1.class);
		job.setMapperClass(MyMapper2.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		Path output_dir = new Path(OUTPUT_PATH);
		FileSystem fs = FileSystem.get(URI.create(url),conf);
		FileStatus stat[] = fs.listStatus(new Path(INPUT_PATH));
		Path files[] = FileUtil.stat2Paths(stat);
		for(Path p : files)
		{
			filename = p.getName();
			LOGGER.info(filename);
			if(filename.equals("empList1.txt"))
			MultipleInputs.addInputPath(job,new Path(INPUT_PATH+"/"+filename),TextInputFormat.class,MyMapper1.class);
			else if(filename.equals("empList2.txt"))
			MultipleInputs.addInputPath(job,new Path(INPUT_PATH+"/"+filename),TextInputFormat.class,MyMapper2.class);
			else
			LOGGER.info("Create Mapper class to the Corresponding input file");
		}
		FileOutputFormat.setOutputPath(job,output_dir);
		output_dir.getFileSystem(conf).delete(output_dir, true);
		flag = job.waitForCompletion(true);
		if(flag)
		{
			MyIOUtils.readOutputFiles(OUTPUT_PATH);
			LOGGER.info("Job execution is successfull");
		}

	}

}
