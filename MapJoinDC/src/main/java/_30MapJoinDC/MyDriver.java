package _30MapJoinDC;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class MyDriver {

	private static final Logger LOGGER = Logger.getLogger(MyDriver.class);
	private static final String INPUT_PATH = "hdfs://localhost:9000/user/hduser/mjoindata";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/user/hduser/mjoin_Output";
	private static final String url = "hdfs://localhost:9000";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("Map side Join");
		job.setJarByClass(MyDriver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileSystem fs = FileSystem.get(URI.create(url),conf);
		FileStatus stat[] = fs.listStatus(new Path(INPUT_PATH));
		Path files[] = FileUtil.stat2Paths(stat);
		for(Path file:files)
		{
			String filename = file.getName();
			if(filename.equals("HistorySalary.txt"))
				FileInputFormat.addInputPath(job,new Path(INPUT_PATH+"/"+filename));
			else if(filename.equals("employee.txt") || filename.equals("department.txt"))
				job.addCacheFile(new URI(INPUT_PATH+"/"+filename));
			else
				LOGGER.info("Invalid File");
		}
		Path out_dir = new Path(OUTPUT_PATH);
		FileOutputFormat.setOutputPath(job,out_dir);
		out_dir.getFileSystem(conf).delete(out_dir, true);
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
