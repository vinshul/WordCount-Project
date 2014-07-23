package org.myorg;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;


public class WordCountNew
{
   /**************************************
	* Map function for the stage 1 and 2 *
	**************************************/
    
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>
    {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();  //So it can change now on runtime
        public void map (LongWritable key,Text value,OutputCollector<Text,IntWritable> output,Reporter reporter)throws IOException
        {
            String line = value.toString();//converting value into string
            StringTokenizer tokenizer = new StringTokenizer(line);//breaking the line into tokens
            while (tokenizer.hasMoreTokens())
            {
                word.set(tokenizer.nextToken());
                output.collect(word,one);//Finally mapping words into key value pairs
            }
        }
    }
	
   /****************************************
	*Reduce function for the stage 1 and 2 *
	****************************************/
	 public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>
    {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
        {
            int sum = 0;
            while (values.hasNext())
                {
                    sum += values.next().get();
                }

             output.collect(key, new IntWritable(sum));
        }

    }
	
	
	
	/***********************************************************************************************************************
				Map1 for the stage3: Takes stage1 output as input and also concatenates _S1 with the frequency of the keys.
	************************************************************************************************************************/
	
	
	public static class Map1Stage3 extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>
    {       
        public void map (LongWritable key,Text value,OutputCollector<Text,IntWritable> output,Reporter reporter)throws IOException
        {
            String [] line=value.toString().split("\t");
            int finalvalue1 = Integer.parseInt(line[1]);
			output.collect(new Text(line[0]),new IntWritable(finalvalue1));
        }
    }
	
	
	/**************************************************************************************************************
		Map2 for the stage3: Takes stage2 output as input and also concatenates _S2 with the frequency of the keys.
	********************************************************************************************************************/
	
	public static class Map2Stage3 extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>
    {
        public void map (LongWritable key,Text value,OutputCollector<Text,IntWritable> output,Reporter reporter)throws IOException
        {
            String [] line=value.toString().split("\t");
            int finalvalue2 = Integer.parseInt(line[1]);
			output.collect(new Text(line[0]),new IntWritable(finalvalue2));           
			}
    }
   
	
	/************************************************************************************
		Reduce function for the stage3 to finally obtain the smaller value ..
	**********************************************************************************/
	
	
	public static class ReduceStage3 extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>
    {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
        {
            if (values.hasNext())
				{   
					int i= values.next().get();
					int j= values.next().get();
					int Wanted_value = i;
					if (i>j)
						{
						   Wanted_value = j;
						}
					output.collect(key, new IntWritable(Wanted_value));
				}
		}
	}
    
	
	/****************************************************************************
				Driver function i.e. Main()
	***************************************************************************/
	
	public static void main(String[] args) throws Exception
    {
		//Job1: For mapping file 1 (stage1).
 	     
		 JobConf conf = new JobConf(WordCountNew.class);
 	     conf.setJobName("wordcount1");
	     conf.setJarByClass(WordCountNew.class);
	     conf.setOutputKeyClass(Text.class);
 	     conf.setOutputValueClass(IntWritable.class);

 	     conf.setMapperClass(Map.class);
 	     conf.setCombinerClass(Reduce.class);
 	     conf.setReducerClass(Reduce.class);

 	     conf.setInputFormat(TextInputFormat.class);
 	     conf.setOutputFormat(TextOutputFormat.class);

	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
 	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

 	     JobClient.runJob(conf);
		 
		 
		 //Job2: For mapping file 2(stage 2).
		 
		 JobConf conf2 = new JobConf(WordCountNew.class);
 	     conf2.setJobName("wordcount2");
	     conf2.setJarByClass(WordCountNew.class);
 	     conf2.setOutputKeyClass(Text.class);
 	     conf2.setOutputValueClass(IntWritable.class);

 	     conf2.setMapperClass(Map.class);
 	     conf2.setCombinerClass(Reduce.class);
 	     conf2.setReducerClass(Reduce.class);

 	     conf2.setInputFormat(TextInputFormat.class);
 	     conf2.setOutputFormat(TextOutputFormat.class);

	     FileInputFormat.setInputPaths(conf2, new Path(args[2]));
 	     FileOutputFormat.setOutputPath(conf2, new Path(args[3]));

 	     JobClient.runJob(conf2);
		 
		 
		 //Job3:Take the outputs of stage1 and stage2 as input for stage3 and finally with new Map and Reduce functions we get the required output.
		 
	     JobConf conf3 = new JobConf(WordCountNew.class);
 	     conf3.setJobName("wordcount3");
	     conf3.setJarByClass(WordCountNew.class);
 	     conf3.setOutputKeyClass(Text.class);
 	     conf3.setOutputValueClass(IntWritable.class);
 	     conf3.setReducerClass(ReduceStage3.class);	     
 	     conf3.setOutputFormat(TextOutputFormat.class);
		 MultipleInputs.addInputPath(conf3, new Path(args[1]),TextInputFormat.class, Map1Stage3.class);
 	     MultipleInputs.addInputPath(conf3, new Path(args[3]),TextInputFormat.class, Map2Stage3.class);
		
	     
 	     FileOutputFormat.setOutputPath(conf3, new Path(args[4]));
		 
		 JobClient.runJob(conf3);

 	   }
}
