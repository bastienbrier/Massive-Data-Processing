package brier.invertedindex;

import java.io.BufferedReader; // import BufferedReader
import java.io.File;
import java.io.FileReader; // import FileReader
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet; // import HashSet
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; // necessary for name of file
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import brier.invertedindex.InvertedIndex2.TokenCounterReduce.CustomCounter;

public class InvertedIndex2 extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new InvertedIndex2(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {	    
	  
	  @SuppressWarnings("deprecation")
	  Job job = new Job(getConf(), "InvertedIndex2");
	  
	  final Configuration conf = job.getConfiguration(); // get configuration
	  conf.set("mapreduce.output.textoutputformat.separator", " "); // http://stackoverflow.com/questions/16614029/hadoop-output-key-value-separator
	       
      job.setJarByClass(InvertedIndex2.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class); // here the output is text, text
      
      job.setMapperClass(TokenCounterMap.class);
      job.setReducerClass(TokenCounterReduce.class);
      
      job.setNumReduceTasks(1); // set to 1 reduce tasks

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      // Print the counter in the output
      System.out.println("CustomCounter.UniqueWords = " + job.getCounters().findCounter(CustomCounter.UniqueWords).getValue());
      
      return 0;
   }
   
   public static class TokenCounterMap extends Mapper<LongWritable, Text, Text, Text> {
      private Text word = new Text(); // output : word
      private Text file = new Text(); // output : name of the file

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	 File stopWordsFile = new File("/home/cloudera/workspace/InvertedIndex/report/stopwords.txt"); // file with stopwords
    	 BufferedReader read = new BufferedReader(new FileReader(stopWordsFile)); // open the bufferreader to read file
    	 
    	 Set<String> stopWordsList = new HashSet<String>();
    	 String stopword = null;
    	 while ((stopword = read.readLine()) != null){ // add the stopwords from file to list
    		 stopWordsList.add(stopword);
    	 }
    	 read.close();
    	 
    	 for (String token: value.toString().replaceAll("[^A-Za-z0-9]"," ").split("\\s+")) {
        	String filename = ((FileSplit) context.getInputSplit()).getPath().getName(); // find the name of file
        	file = new Text(filename);
        	token = token.toLowerCase(); // convert the token to lower case
        	if (!stopWordsList.contains(token)){ // if token is not a stopword
        		word.set(token);
        	}
    		context.write(word, file);
         }
      }
   }

   public static class TokenCounterReduce extends Reducer<Text, Text, Text, Text> {
      
	  enum CustomCounter{UniqueWords}; // custom counter
 
	  @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
         
    	 HashSet<String> hash = new HashSet<String>(); // create a hashset to avoid duplica
    	 
    	 String final_file = new String();
    	 for (Text value : values){
    		 String file = value.toString(); // catch the name of the file
    		
    		 if(hash!=null && hash.contains(file)==false){ // if the hashset for the word does not contain name of file, add it
    			 hash.add(file);
    		 }
    	 }
    	 
    	 for (String value : hash){
    		 final_file = final_file + value + ","; // create the right format for the output
    	 }
    	 
    	 final_file = final_file.substring(0, final_file.length()-1); // delete the final comma

		 if(hash.size() == 1){
			 context.getCounter(CustomCounter.UniqueWords).increment(1); // increment the counter by one
		 }
      }
	
	/** Cleanup - takes places at the end of the Reducer **/
	protected void cleanup(Context context)
	  		throws IOException, InterruptedException{
		  Long final_count = new Long(5); // initiate a long type to store the counter value
		  final_count = context.getCounter(CustomCounter.UniqueWords).getValue(); // create the value of the counter
		  String final_string = String.valueOf(final_count); // convert it to a string
		  context.write(new Text("CustomCounter.UniqueWords ="), new Text(final_string));
	  }
   }
}
