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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.log4j.Logger;


public class MovieRecommendation extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(MovieRecommendation.class);

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new MovieRecommendation(), args);
	System.exit(res);
    }

    public int run(String[] args) throws Exception {
	// FILL HERE
	 Job job = Job.getInstance(getConf(), "NameMovies");
	 job.setJarByClass(this.getClass());
	 job.setMapperClass(NameMovie.MapRatings.class);
	 job.setMapperClass(NameMovie.MapMovies.class);
	 MultipleInputs.addInputPath(job, new Path(args[0]+".t4"), TextInputFormat.class, NameMovie.MapRatings.class);
   	 MultipleInputs.addInputPath(job, new Path(args[1]+"/movies.csv"), TextInputFormat.class, NameMovie.MapMovies.class);
	 FileOutputFormat.setOutputPath(job, new Path(args[0]+".t5"));
	 job.setReducerClass(NameMovie.Reduce.class);
	 job.setOutputValueClass(Text.class);
	 job.setOutputKeyClass(Text.class);
	 return job.waitForCompletion(true) ? 0 : 1 ;
    }    
	    
}
