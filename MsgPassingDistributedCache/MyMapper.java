package _8MsgPassingDistributedCache;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	Logger logger = Logger.getLogger(MyMapper.class);
	
	public MyMapper() {
		
		logger.info("MyMapper()");
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		Path files[] = context.getLocalCacheFiles();
		Configuration conf = context.getConfiguration();
		for(Path file:files)
		{
			Scanner sc = new Scanner(new File(file.getName()));
			
			while(sc.hasNext())
			{
				String val1 =sc.next();
				
			}
		}
	}
	
	@Override
	public void run(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.run(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.map(key, value, context);
	}
	
	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}
	

}
