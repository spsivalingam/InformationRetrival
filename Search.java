/* Name - Sivalingam Subbiah
Email - ssubbiah@uncc.edu

*/

package org.myorg;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.io.*;
import java.util.regex.Pattern;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Search extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(Search.class);
	private static  String query = null; 
	public static void main(String[] args) throws Exception {
		// fetching the input query to search for seperated based on space
		Scanner scanner = new Scanner(System.in);
		System.out.println("Search query :");
		query = scanner.nextLine();
     int res = ToolRunner.run(new Search(), args);
     System.exit(res);
  }
  
  public int run( String[] args) throws  Exception {
	  Configuration conf = new Configuration();
		conf.set("query", query);  // attach the search query along with the config file to pass to the mapper program
      Job job  = Job .getInstance(conf, " search ");
      job.setJarByClass( this.getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
	  
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      
	  // configure the output data format
	  
	  job.setOutputKeyClass(Text .class);
      job.setOutputValueClass(Text.class);

      return job.waitForCompletion(true) ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,Text,Text ,  Text > {
          // is#####fileName value
      

      @Override
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
		 
		 String values[]= line.split("#####");
		 String searchQuery = context.getConfiguration().get("query");
		 String q[] = searchQuery.split(" ");
		 String word= values[0];
         String fnValue = values[1];
		 String fName[] = fnValue.split("\t");
		 for(String query : q){
			 
			 if(query.equalsIgnoreCase(word)){
				 context.write(new Text(fName[0]), new Text(fName[1])); 
			 }
			 
		 }
      }
   }
   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text word,  Iterable<Text > postingsList,  Context context)
         throws IOException,  InterruptedException {
         String sum;
	double count =0;
         for ( Text p  : postingsList) {
            sum= p.toString();
		Double v = Double.valueOf(sum);
		count+=v;
         }
		
         context.write(word,  new Text(Double.toString(count))); // output to be written on the output file
      }
   }
   
}
