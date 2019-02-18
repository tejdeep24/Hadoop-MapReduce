package _AmazonAssignment;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

public class MyNlineInputFormat extends TextInputFormat{
	
	
	
	private Logger logger = Logger.getLogger(MyNlineInputFormat.class);
	
	public MyNlineInputFormat() {
		
		logger.info("MyNlineInputFormat()");
	}
	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		
		logger.info("creating RecordReader object");
		return new MyRecordReader();
	}

}
