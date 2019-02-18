package _AmazonAssignment;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

public class MyIOUtils {
	
	private static Logger logger = Logger.getLogger(MyIOUtils.class);
	private static String url = "hdfs://localhost:9000";
	private static String path = "src/main/resources/data";
	
	
	public static void uploadInputFile(String inpath) throws IOException
	{
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url),conf);
		File file = new File(path);
		File[] files = file.listFiles();
		Path hdfsinpath = new Path(inpath);
		hdfsinpath.getFileSystem(conf).delete(hdfsinpath,true);
		
		for(File f:files)
		{
			logger.info(f.getName());
			InputStream in = new BufferedInputStream(new FileInputStream(path+"/"+f.getName()));
			FSDataOutputStream out = fs.create(new Path(hdfsinpath+"/"+f.getName()));
			IOUtils.copyBytes(in,out,4096,true);
		}
	}
	public static void readOutputFile(String outpath) throws IOException
	{
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url),conf);
		FileStatus stat[] = fs.listStatus(new Path(outpath));
		Path files[] = FileUtil.stat2Paths(stat);
		for(Path file:files)
		{
			logger.info("**************"+file.getName()+"*******************");
			FSDataInputStream in = fs.open(file);
			IOUtils.copyBytes(in,System.out,4096,false);
			
		}
		
	}

}
