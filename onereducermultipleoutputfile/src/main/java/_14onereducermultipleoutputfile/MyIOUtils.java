package _14onereducermultipleoutputfile;

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
	
	private static final String url = "hdfs://localhost:9000/";
	private static InputStream in;
	private static OutputStream out;
	private static final Logger LOGGER = Logger.getLogger(MyIOUtils.class);
	
	public static String uploadInputFiles(String input_path) throws IOException
	{
		String localpath = "src/main/resources";		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url),conf);
		fs.delete(new Path(input_path),true);
		File f = new File(localpath);
		File files[] = f.listFiles();
		
		int i=0;
		for(File fi: files)
		{
			i++;
			String filename = fi.toString();
			LOGGER.info(filename);
			in = new BufferedInputStream(new FileInputStream(filename));
			out = fs.create(new Path(input_path+"/"+filename));
			IOUtils.copyBytes(in, out,4096,true);
			LOGGER.info("Created "+input_path+"/"+filename+" Successfully");
			if(i==2)
				break;
		}
		return input_path+"/"+localpath;
	}
	
	public static void readOutputFiles(String output_path) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url),conf);
		FileStatus stat[] = fs.listStatus(new Path(output_path));
		Path files[] = FileUtil.stat2Paths(stat);
		for(Path filename:files)
		{
			in = fs.open(filename);
			LOGGER.info("*********************Output of "+filename+"*****************");
			IOUtils.copyBytes(in,System.out,4096,false);
		}
	}

}
