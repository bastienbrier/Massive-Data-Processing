package brier.invertedindex;

import java.io.BufferedReader; // import BufferedReader
import java.io.File;
import java.io.FileReader; // import FileReader
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap; // import HashMap
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
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

public class InvertedIndex3 extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new InvertedIndex3(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {	    
	  System.out.println(Arrays.toString(args));
	  
	  @SuppressWarnings("deprecation")
	  Job job = new Job(getConf(), "InvertedIndex3");
	  	       
      job.setJarByClass(InvertedIndex3.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class); // here the output is text, text
      
      job.setMapperClass(TokenCounterMap.class);
      job.setCombinerClass(TokenCounterCombiner.class);
      job.setReducerClass(TokenCounterReduce.class);
      
      job.setNumReduceTasks(1); // set to 1 reduce tasks

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
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
  	  
  	  @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
             	  
    	 HashMap<String, Integer> hash = new HashMap<String, Integer>(); // create a hashmap (key, value)
    	 
    	 String final_file = new String();
    	 for (Text value : values){
    		 String file = value.toString();
    		 
    		 if (hash.get(file)!=null){ // if the hashmap for the word already contains name of file, increment by one
    			 int count = hash.get(file);
    			 hash.put(file, ++count);
    		 }
    		 else{ // else create it with one as value
    			 hash.put(file, 1);
    		 }
    	 }
    	 
    	 for (String value : hash.keySet()){
    		 final_file = final_file + value + "#" + hash.get(value) + ","; // create the right format for the output
    	 }
    	 
    	 final_file = final_file.substring(0, final_file.length()-1); // delete the final comma
    	 context.write(key, new Text(final_file));
      }
   }
   
   /** Create the combiner **/
   public static class TokenCounterCombiner extends Reducer<Text, Text, Text, Text> {
	   public void reduce(Text key, Iterable<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			   throws IOException{

	   }
   }
}