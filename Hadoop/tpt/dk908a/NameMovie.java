package tpt.dk908a;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;


public class NameMovie {
    
    public static class MapRatings extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text moviePair, Context context)
	    throws IOException, InterruptedException {
	    //FILL HERE
	    //
	    String[] kv = moviePair.toString().split("\\t");
	    context.write(new Text(kv[1]), new Text(kv[0]+".0"));
	}
    }

    public static class MapMovies extends Mapper<LongWritable, Text, Text, Text> {
	private static final Pattern p = Pattern.compile("^([0-9]+), ([^,\"]+|\"[^\"+\"), (.+)$");

	public void map(LongWritable offset, Text moviePair, Context context)
	    throws IOException, InterruptedException {
	    //FILL HERE
	    //
   	    boolean b = moviePair.toString().contains("title");
	    if(!b){
		String s = moviePair.toString();
		Matcher m = p.matcher(s);
		if(m.find()){
		    context.write(new Text(m.group(0)), new Text(m.group(1)+".1"));
		}
	    }
	    
	}
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> {	
	@Override
	public void reduce(Text movie, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException {
	    // FILL HERE
	    //
	    String m1 = "";
	    String m2 = "";
	    for(Text v : values){
		String[] s = v.toString().split(".");
		if(s[1].equals("0"))
		    m2 = s[0];
		if(s[1].equals("1"))
		    m1 = s[0];
	    }
	    context.write(new Text(m1), new Text(m2));
	}
    }
}


