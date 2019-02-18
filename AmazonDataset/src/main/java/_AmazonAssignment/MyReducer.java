package _AmazonAssignment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Scanner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

public class MyReducer extends Reducer<Text,Text,Text,Text>{
	
	private static Logger logger = Logger.getLogger(MyReducer.class);
	private static MultipleOutputs<Text,Text> mos;
	private Scanner scan;
	public MyReducer() {
		
		logger.info("MyReducer()");
	}
	
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		
		logger.info("MyReducer setup method");
		mos = new MultipleOutputs<Text,Text>(context);
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.run(context);
		logger.info("MyReducer run method");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException {
	
		Iterator<Text> it = values.iterator();
		String KEY[] = key.toString().split(":");
		String content = null;
		String asin= null;
		String group = null;
		String title = null;
		String salesrank = null;
		String similar = null;
		String cat = null;
		String rev = null;
		String category[];
		String review[];
		ArrayList<String> cr = new ArrayList<String>();
		ArrayList<String> re = new ArrayList<String>();
		while(it.hasNext())
		{
			content = it.next().toString();
		}
		logger.info(KEY[1]);
		scan = new Scanner(content);
		try
		{
		while(scan.hasNext())
		{
			asin = scan.nextLine();
			title = scan.nextLine().trim();
			group = scan.nextLine().trim();
			salesrank = scan.nextLine().trim();
			similar = scan.nextLine().trim();
			cat = scan.nextLine().trim();
			category = cat.split(":");
			int cgrycount = Integer.parseInt(category[1].toString().trim());
			for(int i=0;i<cgrycount;i++)
			{
				cr.add(scan.nextLine());
			}
			
			rev = scan.nextLine().trim();
			review = rev.split(" ");
			int revcount = Integer.parseInt(review[2].trim());
			for(int i=0;i<revcount;i++)
			{
				re.add(scan.nextLine().trim());
			}
			break;
		}
		}
		catch(NoSuchElementException e)
		{
			mos.write(new Text(KEY[1]),new Text(asin),"result3.txt");
			return;
		}
		scan.close();
		mos.write(new Text(KEY[1]),new Text(asin+"^"+title+"^"+group+"^"+salesrank),"result1.txt");
		mos.write(new Text(KEY[1]),new Text(similar),"result2.txt");
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		logger.info("MyReducer cleanup method");
		mos.close();
	}
	
	

}
