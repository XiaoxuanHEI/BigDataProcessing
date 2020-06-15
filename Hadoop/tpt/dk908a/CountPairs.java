package tpt.dk908a;

import java.io.IOException;
import java.util.regex.Pattern;
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


public class CountPairs {
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text moviePair, Context context)
	    throws IOException, InterruptedException {
	    //FILL HERE
	    //
  	    String[] kv = moviePair.toString().split("\\t");
	    String[] movies = kv[1].split(",");
	    for(int i=0; i<movies.length; i++){
		for(int j=1; j<movies.length; j++){
		    if(movies[i].compareTo(movies[j])>0)
			context.write(new Text(movies[i] + "," + movies[j]), new Text("1"));
		    else
			context.write(new Text(movies[j] + "," + movies[i]), new Text("1"));
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
	    int count = 0;
	    for(Text v : values)
		count++;
	    context.write(movie, new Text(Integer.toString(count)));
	}
    }
}


