import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class movie {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word ; //type of output key
		private Text title;
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
		{
			String[] tokens = value.toString().split("::");
			Configuration conf = context.getConfiguration();
			String gender = conf.get("test")+"\t"+tokens[1];
			word = new Text(gender);
			context.write(word, one);
			
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable,Text,IntWritable>
	{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable val : values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
			
		}
	}

	public static void main(String[] args)throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String [] otherArgs = new GenericOptionsParser(conf,args ).getRemainingArgs();
		conf.set("test",otherArgs[2]);
		//get all args
//		if(otherArgs.length!=2)
//		{
//			System.err.println("Usage: age group <in> <out>");
//			System.exit(2);
//		}
		Job job = new Job(conf,"movie");
		job.setJarByClass(Map.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//no combiner used
		job.setOutputKeyClass(Text.class);//set output key type
		job.setOutputValueClass(IntWritable.class); //set output value type
		//set HDFS path of input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		//set HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0:1);

	}

}
