package brier.invertedindex;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class StopWords3 extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new StopWords3(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {	   
	  System.out.println(Arrays.toString(args));
      
	  @SuppressWarnings("deprecation")
	  Job job = new Job(getConf(), "StopWords3");
	  
	  final Configuration conf = job.getConfiguration(); // get configuration
	  conf.set("mapreduce.output.textoutputformat.separator", ","); // http://stackoverflow.com/questions/16614029/hadoop-output-key-value-separator
	  conf.setBoolean("mapreduce.map.output.compress", true); // http://comphadoop.weebly.com/ + info depreciation terminal
	  conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");   
	  
      job.setJarByClass(StopWords3.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      
      job.setMapperClass(TokenCounterMap.class);
      job.setCombinerClass(TokenCounterCombiner.class); // http://hadooptutorial.info/combiner-in-mapreduce/
      job.setReducerClass(TokenCounterReduce.class);
      
      job.setNumReduceTasks(10); // set to 10 reduce tasks

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class TokenCounterMap extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  for (String token: value.toString().replaceAll("[^A-Za-z0-9]"," ").split("\\s+")) { // replace characters
            token = token.toLowerCase(); // convert the token to lower case
        	word.set(token); 
            context.write(word, ONE);
         }
      }
   }

   public static class TokenCounterReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         if (sum > 4000) {
        	context.write(key, new IntWritable(sum)); // only write the words that appear more than 4000 times
         }
      }
   }
   /** Create the combiner **/
   public static class TokenCounterCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
	   public void reduce(Text key, Iterable<IntWritable> values, Context context)
			   throws IOException, InterruptedException{
	       int sum = 0;
	       for (IntWritable val : values) {
	            sum += val.get();
	         }
	       context.write(key, new IntWritable(sum));
	   }
   }
}
