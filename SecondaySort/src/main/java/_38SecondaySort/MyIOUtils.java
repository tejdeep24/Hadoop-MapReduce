package _38SecondaySort;

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
	
	public static void uploadInputFiles(String input_path) throws IOException
	{
		String local_path = "src/main/resources/input_data";
		File f = new File(local_path);
		File files[] = f.listFiles();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url),conf);
		fs.delete(new Path(input_path),true);
		for(File file:files)
		{
			String filename = file.getName();
			in = new BufferedInputStream(new FileInputStream(local_path+"/"+filename));
			out = fs.create(new Path(input_path+"/"+filename));
			IOUtils.copyBytes(in,out,4096,true);
			LOGGER.info("Created "+input_path+"/"+filename);
		}
		
	}
	public static void readOutputFiles(String output_path) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url),conf);
		FileStatus stat[] = fs.listStatus(new Path(output_path));
		Path files[] = FileUtil.stat2Paths(stat);
		for(Path file:files)
		{
			String filename = file.getName();
			in = fs.open(new Path(output_path+"/"+filename));
			LOGGER.info("***********OUTPUT OF"+filename+"*******************");
			IOUtils.copyBytes(in,System.out,4096,false);
		}
	}

}
