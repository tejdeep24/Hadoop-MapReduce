package _17messagepassingusingconf;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;


public class MyIOUtils {
	
	private static final Logger LOGGER = Logger.getLogger(MyIOUtils.class);
	private static final String url = "hdfs://localhost:9000";
	private static InputStream in;
	private static OutputStream out;
	private static Configuration conf;
	private static FileSystem fs;
	
	public static void uploadInputFiles(String input_path) throws IOException
	{
		String localpath = "src/main/resources/data";
		File f = new File(localpath);
		File filelist[] = f.listFiles();
		
		conf = new Configuration();
		fs = FileSystem.get(URI.create(url),conf);
		fs.delete(new Path(input_path),true);
		
		for(File listfile:filelist)
		{
			String filename = listfile.getName();
			in = new  BufferedInputStream(new FileInputStream(localpath+"/"+filename));
			out = fs.create(new Path(input_path+"/"+filename));
			IOUtils.copyBytes(in, out,4096,true);
			LOGGER.info("created"+input_path+"/"+filename);
		}
	}
	
	public static void readOutputFiles(String output_path) throws IOException
	{
		conf = new Configuration();
		fs = FileSystem.get(URI.create(url),conf);
		
		FileStatus stat[] = fs.listStatus(new Path(output_path));
		
		Path files[] = FileUtil.stat2Paths(stat);
		
		for(Path filename : files)
		{
			in = fs.open(filename);
			IOUtils.copyBytes(in,System.out,4096,false);
		}
		
	}

}
