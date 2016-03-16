/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class CountJob {

	public static class TokenizerMapper<K> extends Mapper<K, Text, Text, LongWritable> {
		private Pattern pattern;
		  public static String PATTERN = "mapreduce.mapper.regex";

		private Text word = new Text();
		  public void setup(Context context) {
			    Configuration conf = context.getConfiguration();
			    //pattern = Pattern.compile(conf.get(PATTERN));
			    pattern = Pattern.compile(conf.get(PATTERN));

			  }
  public void map(K key, Text value,Context context
          ) throws IOException, InterruptedException {
        	  //StringTokenizer itr = new StringTokenizer(value.toString());
        	  if(value.toString()!=null&&value.toString()!=""&&value.getLength()>0&&!value.toString().isEmpty())
        		  {
				JsonParser parser=new JsonParser();

	        	  JsonObject json=parser.parse(value.toString()).getAsJsonObject();
        	  
        	  if(json.has("text"))
        		  {
        			  String textContent=json.get("text").getAsString();
        			  String[] texts=textContent.split("\\W");
			    		  //System.out.println(pattern.toString()+"\n");
			    		  //System.err.append(pattern.toString()+"\n");

        			  for(String itr:texts)
        				  {
  	        			      Matcher matcher = pattern.matcher(itr);
  	        			      if(matcher.find())
  	        			    	  {
  	        			    		  //System.out.println(textContent+"\n");
  	      			    		  //System.err.append(textContent+"\n");
  	      			    	word.set(textContent);
    							  context.write(word, new LongWritable(1));

  	        			    	  }
        					  if(itr!=""&&itr!=null&&!itr.isEmpty())
        						  {
        							  //word.set(itr);
        							  //context.write(word, new LongWritable(1));
        							  
        						  }
        				  }
        		  }
        		  }
  		}

}
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  public static void doJob(String param,String args[],String msgs) throws IOException, ClassNotFoundException, InterruptedException
	  {
		  Configuration conf = new Configuration();
		  conf.set(TokenizerMapper.PATTERN, args[2]);
		  FileSystem hdfs =FileSystem.get(conf);
		  Path tempOutput1=new Path("/data/output/temp/"+param+"1");
		  Path tempOutput2=new Path("/data/output/temp/"+param+"2");
		  if(hdfs.exists(tempOutput1)||hdfs.exists(tempOutput2))
			  {
				  hdfs.delete(tempOutput1,true);
			      hdfs.delete(tempOutput2,true);  
			  }
		  

		  Job job = new Job(conf, "word count");
		    job.setJarByClass(CountJob.class);
		    job.setMapperClass(TokenizerMapper.class);
		    job.setCombinerClass(LongSumReducer.class);
		    job.setReducerClass(LongSumReducer.class);
		    job.setOutputFormatClass(SequenceFileOutputFormat.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, tempOutput1);
		    job.waitForCompletion(true);
		    
		    Job sortJob1 = new Job(conf);
		    sortJob1.setJobName("grep-sort");
		    FileInputFormat.setInputPaths(sortJob1, tempOutput1);
		    sortJob1.setInputFormatClass(SequenceFileInputFormat.class);
		    sortJob1.setMapperClass(InverseMapper.class);
		    sortJob1.setNumReduceTasks(1);                 // write a single file
		    FileOutputFormat.setOutputPath(sortJob1,tempOutput2);
		    sortJob1.setSortComparatorClass(          // sort by decreasing freq
		    LongWritable.DecreasingComparator.class);
		    sortJob1.waitForCompletion(true);
			hdfs.delete(tempOutput1,true);

	  }

  public static void main(String[] args) throws Exception  {
		 Configuration conf = new Configuration();
	  		String msgs="";
			doJob("1",args,msgs);
			doJob("2",args,msgs);
			  FileSystem hdfs =FileSystem.get(conf);	
			  

	    
	    

		      
         BufferedReader bfr=new BufferedReader(new InputStreamReader(hdfs.open(new Path("/data/output/temp/12/part-r-00000"))));     
         BufferedReader bfr2=new BufferedReader(new InputStreamReader(hdfs.open(new Path("/data/output/temp/22/part-r-00000"))));     
         Boolean same=true;
         String line1;
         String line2;
         line1=bfr.readLine();
         line2=bfr2.readLine();
         while(same==true)
        	 {
        		if((line1==null&&line2!=null)||(line1!=null&&line2==null))
        			{
        				same=false;
        				break;
        			}
        		else if((line1==null&&line2==null))
        			{
        				break;
        			}
        		else
        			{
        				if(line1.equals(line2))
        					{
        						line1=bfr.readLine();
        				         line2=bfr2.readLine();
        					}
        				else {
        					same=false;
        					break;
        				}
        			}
        	 }
         if(same==true)
        	 {
        		 System.out.print("same "+same+"\n");
        		 Path localP=new Path("/tmp/output.txt");
        		 hdfs.copyToLocalFile(new Path("/data/output/temp/12/part-r-00000"), localP);
        		 hdfs.copyFromLocalFile(localP, new Path(args[1]+"/part-r-00000"));
        		 hdfs.createNewFile(new Path(args[1]+"/_SUCCESS"));
        		 System.out.print("created result");

        	 }
         else
        	 {
        		 
        		 System.out.print("Different");
     			 doJob("3",args,msgs);
        		 Path localP=new Path("/tmp/output.txt");
        		 hdfs.copyToLocalFile(new Path("/data/output/temp/32/part-r-00000"), localP);
        		 hdfs.copyFromLocalFile(localP, new Path(args[1]+"/part-r-00000"));
        		 hdfs.createNewFile(new Path(args[1]+"/_SUCCESS"));
        		 System.out.print("created result");
        		 

        	 }
         hdfs.delete(new Path("/data/output/temp/12/part-r-00000"),true);
         hdfs.delete(new Path("/data/output/temp/22/part-r-00000"),true);

         }
}
