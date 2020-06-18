package _AmazonAssignment;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

public class MyRecordReader extends RecordReader<LongWritable,Text> {
	
	private Logger logger = Logger.getLogger(MyRecordReader.class);
	private long start;
	private long end;
	private FSDataInputStream fsin;
	private LongWritable key;
	private Text value;
	private DataOutputBuffer buffer;
	private String startTag;
	private String endTag;
	private int count;
	private long pos;
	private StringBuffer sb;
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		
		logger.info("RecordReader intialize method");
		Configuration conf = context.getConfiguration();
		startTag = conf.get("Starttag");
		endTag = conf.get("endtag");
		key = new LongWritable();
		value = new Text();
		buffer = new DataOutputBuffer();
		sb = new StringBuffer();
		FileSplit filesplit = (FileSplit)split;
		start = filesplit.getStart();
		end =start+filesplit.getLength();
		logger.info("Start Postition:"+start+"End Position:"+end);
		Path file = filesplit.getPath();
		FileSystem fs =file.getFileSystem(conf);
		fsin = fs.open(file);
		fsin.seek(start);
		pos = start;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		logger.info("RecordReader nextKeyValue method");
		while(fsin.getPos() < end)
		{
			String currentline = fsin.readLine();
			key.set(pos);
				if(currentline.contains(startTag))
				{
					sb.append(currentline);
					sb.append("\n");
					continue;
				}
				if(currentline.isEmpty())
				{
					count++;
				}
				if(count < 2 && !currentline.isEmpty())
				{
					sb.append(currentline);
					sb.append("\n");
				}
				if(count == 2)
				{
					count =1;
					pos=fsin.getPos();
					value.set(sb.toString());
					sb.setLength(0);
					return true;
				}
		}
		return false;
	}
	
	@Override
	public LongWritable getCurrentKey() throws IOException,InterruptedException {
		
		logger.info("RecordReader getCurrentKey()");
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		
		logger.info("RecordReader getCurrentValue()");
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		
		logger.info("RecordReader getprogress method");
		return fsin.getPos()/end;
	}

	@Override
	public void close() throws IOException {
		
		logger.info("RecordReader close method");
		fsin.close();
		
	}

}
