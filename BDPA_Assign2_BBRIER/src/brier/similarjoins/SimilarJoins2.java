package brier.similarjoins;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

public class SimilarJoins2 extends Configured implements Tool {
   static double threshold = 0.8; // threshold value
   static File inputFile = new File("/home/cloudera/workspace/SimilarJoins/input/input_sample.txt");
   
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new SimilarJoins2(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      @SuppressWarnings("deprecation")
      Job job = new Job(getConf(), "SimilarJoins2");
      job.setJarByClass(SimilarJoins2.class);
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
     
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	
    	long line_count = context.getCounter(CustomCounter.currentLine).getValue(); // get id
    	String[] token = value.toString().split("\\s+");   	
    	
    	int doc_length = token.length - 1; // number of words minus the id
    	int numberWordsToComp = doc_length - (int) Math.ceil(threshold * (double) doc_length) + 1;
    	
    	context.getCounter(CustomCounter.currentLine).increment(1); // increment counter
    	for(int i = 1; i < numberWordsToComp + 1; i++){
    		context.write(new Text(token[i].toString().toLowerCase()), new Text(String.valueOf(line_count)));
    	}
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
	  HashMap<String, Double> comparisons = new HashMap<String, Double>(); // initialize HashMap that checks what we compared;
	  HashMap<Integer, String> id_docs_red = new HashMap<Integer, String>();
	  
  	  protected void setup(Context context) throws IOException, InterruptedException {
  		  
  	  	  BufferedReader read = new BufferedReader(new FileReader(inputFile)); // open the bufferreader to read file 
     	  String line = null;
     	  while ((line = read.readLine()) != null){
     		  String[] token = line.toString().split("\\s+");
     		  int doc_id = Integer.valueOf(token[0]);
     		  StringBuilder document = new StringBuilder();
          	  for(int i = 1; i < token.length; i++){
          		  document.append(token[i]);
          		  if(i != token.length - 1){ // to delete final space
          			  document.append(" "); // space between words
          		  }
          	  }
          	id_docs_red.put(doc_id, document.toString());
     	  }
     	  read.close();     	
  	  }
  	  
	  public void reduce (Text key, Iterable<Text> value, Context context)
              throws IOException, InterruptedException {
		  
		  ArrayList<String> ids = new ArrayList<String>();
		  
		  for(Text token : value){
			  ids.add(token.toString()); // add the different ids associated with the word
		  }
		  
		  for(int i = 0; i < ids.size(); i++){
			  for(int j = i + 1; j < ids.size(); j++){
				  int doc_id1 = 0;
				  int doc_id2 = 0;
				  if(Integer.valueOf(ids.get(i)) < Integer.valueOf(ids.get(j))){ // order, smaller ID before
					  doc_id1 = Integer.valueOf(ids.get(i));
					  doc_id2 = Integer.valueOf(ids.get(j));
				  }
				  else{
					  doc_id1 = Integer.valueOf(ids.get(j));
					  doc_id2 = Integer.valueOf(ids.get(i));
				  }
				  String compString = "(" + String.valueOf(doc_id1) + "," + String.valueOf(doc_id2) + ")";
				  if(comparisons.get(compString) == null){ // if we have not already compared them
					  HashSet<String> doc_1 = new HashSet<String>(Arrays.asList(id_docs_red.get(doc_id1).toLowerCase().split("\\s+")));
					  HashSet<String> doc_2 = new HashSet<String>(Arrays.asList(id_docs_red.get(doc_id2).toLowerCase().split("\\s+")));
					  double similarity = sim(doc_1, doc_2);
					  comparisons.put(compString, similarity); // add to the HashMap to remember what we compared
					  context.getCounter(CustomCounter.comparisonsNumber).increment(1); // increment counter for nb of comp
					  if(similarity >= threshold){
						  context.write(new Text(compString), new Text(String.valueOf(similarity))); // write if > threshold
					  }
				  }
			  }
		  } 
      }
   }
}

