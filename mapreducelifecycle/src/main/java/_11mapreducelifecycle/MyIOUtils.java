package _11mapreducelifecycle;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

public class MyIOUtils {
	
	private static final String url = "hdfs://localhost:9000";
	private static InputStream in ;
	private static OutputStream out;
	private static final Logger LOGGER = Logger.getLogger(MyIOUtils.class);
	
	public static void uploadInputFile(String input_path) throws IOException
	{
		String localpath = "src/main/resources";
		String Filenames[] = {"word1.txt","word2.txt"};
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url),conf);
		fs.delete(new Path(input_path),true);
		
		for(String filename: Filenames)
		{
			in = new BufferedInputStream(new FileInputStream(localpath+"/"+filename));
			out = fs.create(new Path(input_path+"/"+filename));
			IOUtils.copyBytes(in, out,4096,true);
			LOGGER.info("created "+input_path+"/"+filename+" Successfully");
		}
		
		
	}
	
	public static void readOutputFile(String output_path) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url),conf);
		
		in = fs.open(new Path(output_path+"/part-r-00000"));
		IOUtils.copyBytes(in,System.out,4096,true);
	}

}
