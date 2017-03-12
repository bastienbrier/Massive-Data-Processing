package brier.similarjoins;

import java.io.BufferedReader; // import BufferedReader
import java.io.File;
import java.io.FileReader; // import FileReader
import java.io.IOException;
import java.util.ArrayList; // import ArrayList
import java.util.Arrays;
import java.util.Collections; // import Collections
import java.util.HashMap; // import HashMap
import java.util.HashSet; // import HashSet
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Input_Preprocessing extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Input_Preprocessing(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      @SuppressWarnings("deprecation")
      Job job = new Job(getConf(), "Input_Preprocessing");
      job.setJarByClass(Input_Preprocessing.class);
      job.setOutputKeyClass(LongWritable.class);
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
   
   public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
      private Text word = new Text();
 	 
     File stopWordsFile = new File("/home/cloudera/workspace/SimilarJoins/report/stopwords.txt"); // file with stopwords
     Set<String> stopWordsList = new HashSet<String>();
     
     /** Setup : read and store the stopwords before the mapper **/
  	 protected void setup(Context context) throws IOException, InterruptedException {
    	 
  	  	 BufferedReader read = new BufferedReader(new FileReader(stopWordsFile)); // open the bufferreader to read file 
     	 String stopword = null;
     	 while ((stopword = read.readLine()) != null){ // add the stopwords from file to list
     		 stopWordsList.add(stopword);
     	 }
     	 read.close();
	 }
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	 
 		List <String> wordsCount = new ArrayList <>();
 		if (value.toString().length() != 0){ // delete empty lines
            for (String token: value.toString().replaceAll("[^A-Za-z0-9]"," ").split("\\s+")){ // replace non character values
            	if (!stopWordsList.contains(token.toLowerCase())){ // if token is not a stopword
            		if (!wordsCount.contains(token.toLowerCase())){ // if word has not been already in the line
            			wordsCount.add(token.replaceAll("[^A-Za-z0-9]","").toString().toLowerCase()); // add in the list to campare
            			word.set(token.toString());
            			if(word.toString().length() != 0){
            				context.write(key, word);
            			}
            		}
            	}
            }
 		}
      }
   }

   public static class Reduce extends Reducer<LongWritable, Text, Text, Text> {
     
	 enum CustomCounter{TotalLines}; // initialize counter
	   
	 File wordcountFile = new File("/home/cloudera/workspace/SimilarJoins/report/wordcount.txt"); 
  	 HashMap<String, Integer> wordCount = new HashMap<String, Integer>();  
  	 
  	/** Setup : read and store the wordcount before the reducer **/
  	 protected void setup(Context context) throws IOException, InterruptedException {
     	 BufferedReader read = new BufferedReader(new FileReader(wordcountFile));
     	 String word_c = null;
     	 while ((word_c = read.readLine()) != null){ 
     		 String[] word_value = word_c.split(","); // add word and value
     		 int value_count = Integer.parseInt(word_value[1]);
     		 wordCount.put(word_value[0], value_count);
     	 }
     	 read.close();
	 }
	   
	  public void reduce(LongWritable key, Iterable<Text> value, Context context)
              throws IOException, InterruptedException {
     	 		  
		 ArrayList<String> wordPhrase = new ArrayList<String>();
		 ArrayList<Integer> wordPhraseCount = new ArrayList<Integer>();
		 for (Text word : value){
			 wordPhrase.add(word.toString()); // add the words of id in a list
			 wordPhraseCount.add(wordCount.get(word.toString().toLowerCase())); // and add their related frequencies in another
		 }
		 
		 ArrayList<String> wordPhraseFinal = new ArrayList<String>(); // final list with ordered words as frequency
		 int count = 0;
		 while(count < wordPhrase.size()){
			 for(int i = 0; i < wordPhrase.size(); i++){
				 String cur_word = wordPhrase.get(i);
				 if(wordPhraseCount.get(i) == Collections.min(wordPhraseCount)){
					 wordPhraseFinal.add(cur_word); // add the min frequency and set its frequency to 5000 to not be selected again
					 wordPhraseCount.set(i,5000);
					 count++; // increment count
				 }
			 }
		 }
     	 StringBuilder final_phrase = new StringBuilder(); // final output
     	 for (int i = 0; i < wordPhrase.size(); i++){
     		 final_phrase.append(wordPhraseFinal.get(i).toString());
     		 //final_phrase.append("#"); // uncomment if you want to see the frequency
     		 //final_phrase.append(wordCount.get(wordPhraseFinal.get(i).toString().toLowerCase()));
     		 final_phrase.append(" ");
     	 }
     	 long line_count = context.getCounter(CustomCounter.TotalLines).getValue();
     	 context.getCounter(CustomCounter.TotalLines).increment(1); // increment counter
     	 context.write(new Text(String.valueOf(line_count)), new Text(final_phrase.toString()));
      }
   }
}

