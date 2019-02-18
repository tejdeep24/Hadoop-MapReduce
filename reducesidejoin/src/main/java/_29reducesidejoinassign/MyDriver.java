package _29reducesidejoinassign;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class MyDriver {
	
	private static final Logger LOGGER = Logger.getLogger(MyDriver.class);
	private static final String INPUT_PATH = "hdfs://localhost:9000/user/hduser/rjoinasgndata";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/user/hduser/rjoinasgndata_output";
	private static boolean flag;
	private static String url = "hdfs://localhost:9000";
	private static String filename;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		MyIOUtils.uploadInputFiles(INPUT_PATH);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.getConfiguration().set("mapred.textoutputformat.separator",":");
		job.setJobName("ReducesideJoins");
		job.setJarByClass(MyDriver.class);
		job.setMapperClass(CustomerMapper.class);
		job.setMapperClass(TransactionMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(0);
		FileSystem fs = FileSystem.get(URI.create(url),conf);
		FileStatus stat[] = fs.listStatus(new Path(INPUT_PATH));
		Path listfiles[] = FileUtil.stat2Paths(stat);
		for(Path file:listfiles)
		{
			filename = file.getName();
			LOGGER.info(filename);
			if(filename.equals("customer.txt"))
				MultipleInputs.addInputPath(job,new Path(INPUT_PATH+"/"+filename),TextInputFormat.class,CustomerMapper.class);
			else if(filename.equals("Transactions.txt"))
				MultipleInputs.addInputPath(job,new Path(INPUT_PATH+"/"+filename),TextInputFormat.class,TransactionMapper.class);
			else
				LOGGER.info("Create Mapper class to the Corresponding input file");
		}
		Path output_dir = new Path(OUTPUT_PATH);
		FileOutputFormat.setOutputPath(job,output_dir);
		output_dir.getFileSystem(conf).delete(output_dir, true);
		flag = job.waitForCompletion(true);
		
		if(flag)
		{
			MyIOUtils.readOutputFiles(OUTPUT_PATH);
			LOGGER.info("Job executed successfully");
		}

	}

}
