/* 
sivalingam Subbiah 
ssubbiah@uncc.edu */
package org.myorg;
import java.io.IOException;
import java.io.*;
import java.util.regex.Pattern;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class TFIDF extends Configured implements Tool {
	
	private static final Logger Log = Logger.getLogger(TFIDF.class);
	
	private static long TOTAL_NO_OF_DOCS = 0;
	
	
	
	public static void main(String[] args) throws Exception {
		
		
		
		int y = ToolRunner.run(new TermFrequency(), args);
		if (y == 0) {
			int res = ToolRunner.run(new TFIDF(), args);
			System.exit(res);
		}
		
		if(args.length!=3){
			System.err.println("Enter valid arguments");
			System.exit(0);
		}
     
  }
  
  public int run( String[] args) throws  Exception {
	    Configuration conf = getConf();
	    FileSystem fs = FileSystem.get(conf);
	    Path input = new Path(args[0]);  // input Path
		ContentSummary cs = fs.getContentSummary(input);
		TOTAL_NO_OF_DOCS = cs.getFileCount(); // Determine the Number of input files
		
		conf.set("TotalDocumentsCount", String.valueOf(TOTAL_NO_OF_DOCS)); // pass the  obtained docs count to the conf
	  
	  
	  
	  // Job1
	  
      Job job  = Job.getInstance(conf, " TFIDF ");
      job.setJarByClass( this.getClass());
	 // JobControl jbcontrol = new JobControl("jbcontrol");
     
      job.setMapperClass(Map1.class);
      job.setReducerClass(Reduce1.class);

// configure the key value pairs for map output and reducer output
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);

	  
	  
	   FileInputFormat.addInputPath(job,  new Path(args[1]));
      FileOutputFormat.setOutputPath(job,  new Path(args[2]));

    //  return job.waitForCompletion(true);
	  
	  // Job2
	/*  
	  Job job2  = Job.getInstance(conf, " TFIDF_Job2 ");
      job2.setJarByClass( this.getClass());

      FileInputFormat.addInputPath(job2, new Path(args[1]) );
      FileOutputFormat.setOutputPath(job2, new Path(args[2])  );
	  
      job2.setMapperClass(Map2.class);
      job2.setReducerClass(Reduce2.class);
	  
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);
	 */ 

      return job.waitForCompletion(true) ? 0 : 1; 
	  
   }
   
   
   
   // Mapper 2
   public static class Map1 extends Mapper<LongWritable ,Text,Text , Text > {
      
      

     

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
			Text currentWord  = new Text();
			Text currentValue  = new Text();
         String line  = lineText.toString();
		 String tokens[] = line.split("#####"); // split
		 String interKey = tokens[0];   //yellow
			String a = tokens[1].replaceAll("\\s+","=");
			
		// String splitter[] = tokens[1].split("\t");  

		 //String interValFname = splitter[0];  // Filename
		 //String interValTF= splitter[1];      // TF Value

		// String interVal = interValFname+"="+interValTF; 
 
		 currentWord = new Text(interKey);           
		 currentValue = new Text(a);

		 context.write(currentWord,currentValue);  // key -> word, value -> fileName=TF
		 
      }
   }
   
   // Reducer 2
   
   public static class Reduce1 extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text text,  Iterable<Text> postingsList,  Context context)       //text -> yellow,  postingsList -> fileName=TF Value (collection)
         throws IOException,  InterruptedException {

			 double totalNumDocs =0.0;
			 double docsContainingTerm=0.0;
			 double idf=0.0;
			 double tdIDF=0.0;

			 java.util.Map<String,String> wfLog =new HashMap<>();  // map to store key -> filename, value-> tfvalue
			 String finalKey,finalValue;

			 String total = context.getConfiguration().get("TotalDocumentsCount");
			 totalNumDocs = Double.valueOf(total); // fetching the docs count from the config

			String key = text.toString();  //"Word"


			 for(Text t : postingsList){ // iterates based on the number of docs containing the word

				 docsContainingTerm++; 

				 String valuesFnL[]=t.toString().split("=");  // splitting fileName and LogValue

				 finalKey=key+"#####"+valuesFnL[0];  //is#####fileName

				 wfLog.put(finalKey,valuesFnL[1]);  // pair(word/file, TFvalue)
				 
			 }
			
			 idf = Math.log10(1.0 + (totalNumDocs / docsContainingTerm)); // idf calcualted
			 Log.info("docsContaingTerm"+docsContainingTerm+"total Number of docs"+totalNumDocs);
			 
			 
			 for (java.util.Map.Entry<String, String> entry : wfLog.entrySet()) {

				double tf = Double.valueOf(entry.getValue());

				 tdIDF = tf*idf;  // TFvalue * IDF
				
				String key1 = entry.getKey();
				
				
				context.write(new Text(key1), new DoubleWritable(tdIDF));
				
			}
			
			
			         
      }
   }
   
}
