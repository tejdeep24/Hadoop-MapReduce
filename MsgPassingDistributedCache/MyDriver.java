package _8MsgPassingDistributedCache;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class MyDriver {
	
	static Logger logger = Logger.getLogger(MyDriver.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MyDriver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.addCacheFile(URI.create("hdfs://localhost:9000/user/hduser/MapReduce/data/mydata.txt"));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path input_dir = new Path("hdfs://localhost:9000/user/hduser/MapReduce/data/mydriver.properties");
		Path output_dir = new Path("hdfs://localhost:9000/user/hduser/MapReduce/msgpassing");
		
		output_dir.getFileSystem(conf).delete(output_dir, true);
		
		FileInputFormat.addInputPath(job,input_dir);
		FileOutputFormat.setOutputPath(job,output_dir);
		
		boolean flag = job.waitForCompletion(true);
		
		if(flag)
		{
			logger.info("Job is Successful");
		}
		else
		{
			logger.info("Job is Failed");
		}
	}

}
