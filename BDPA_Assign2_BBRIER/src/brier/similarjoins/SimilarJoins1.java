package brier.similarjoins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class SimilarJoins1 extends Configured implements Tool {
   static double threshold = 0.8; // threshold value
	
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new SimilarJoins1(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      @SuppressWarnings("deprecation")
      Job job = new Job(getConf(), "SimilarJoins1");
      job.setJarByClass(SimilarJoins1.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      
      job.setNumReduceTasks(1); // set to 1 reduce tasks

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	  enum CustomCounter{currentLine}; // initialize counter
      ArrayList<String> all_docs = new ArrayList<String>();
     
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	
    	long line_count = context.getCounter(CustomCounter.currentLine).getValue();
    	String[] token = value.toString().split("\\s+"); 
    	
    	StringBuilder document = new StringBuilder();
    	for(int i = 1; i < token.length; i++){
    		document.append(token[i]);
    		if(i != token.length - 1){ // to delete final space
    			document.append(" "); // space between words
    		}
    	}
    	all_docs.add(document.toString());

    	for(int i = 0; i < all_docs.size() - 1; i++){
    		StringBuilder pair = new StringBuilder();
    		StringBuilder two_docs = new StringBuilder();
    		String doc_1 = all_docs.get(i);
    		String doc_2 = all_docs.get((int) line_count);
			pair.append("(");
    		pair.append(String.valueOf(i)); // pair of docs to be compared
			pair.append(",");
			pair.append(String.valueOf(line_count));
			pair.append(")");
			two_docs.append(doc_1.toLowerCase()); // contents of the two docs
			two_docs.append("#");
			two_docs.append(doc_2.toLowerCase());
			context.write(new Text(pair.toString()), new Text(two_docs.toString()));
    	}
    	context.getCounter(CustomCounter.currentLine).increment(1); // increment counter
      }
   }

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      
	  public double sim(HashSet<String> doc1, HashSet<String> doc2){
    	  HashSet<String> inter_set = new HashSet<String>(doc1); // hashset ensures no duplicates
    	  inter_set.retainAll(doc2); // retain only elements present in doc2
    	  int inter = inter_set.size(); // number of elements in both
    	  HashSet<String> union_set = new HashSet<String>(doc1);
    	  union_set.addAll(doc2);
    	  int union = union_set.size(); // number of elements in one or another
    	  double similarity = ((double) inter) / ((double) union); // jaccard sim formula
    	  return similarity;
      }
      
	  enum CustomCounter{comparisonsNumber}; // initialize counter
	  
	  public void reduce(Text key, Iterable<Text> value, Context context)
              throws IOException, InterruptedException {
	     
		 for(Text token : value){
			 String[] docs = token.toString().split("#"); 
		     HashSet<String> doc_1 = new HashSet<String>(Arrays.asList(docs[0].split("\\s+")));
		     HashSet<String> doc_2 = new HashSet<String>(Arrays.asList(docs[1].split("\\s+")));
		     double similarity = sim(doc_1, doc_2);
		     context.getCounter(CustomCounter.comparisonsNumber).increment(1); // increment counter
		     if(similarity >= threshold){
		    	 context.write(new Text(key), new Text(String.valueOf(similarity)));
		     }
	     }
      }
   }
}
