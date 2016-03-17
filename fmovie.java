import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/*
 * do twice join, distributed cache failed
 */
public class fmovie {
	private static Map<String,String> mymap = new HashMap<String,String>();
		public static class Map1 extends Mapper<LongWritable, Text, Text, Text>
		{//take care of rating data

			
			private Text rating;
			private Text usrid = new Text();  // type of output key 
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String[] mydata = value.toString().split("::");
				///	System.out.println(value.toString());
				String intrating = "r"+mydata[1]+"~"+mydata[2];//get the movieID and rating
				rating = new Text( intrating);
				usrid.set(mydata[0].trim());
				context.write(usrid, rating);


			}


		}



		//second mapper function
		public static class Map2 extends Mapper<LongWritable, Text, Text, Text>
		{//take care of users data
			private Text usrid = new Text();
			private Text one = new Text();  // type of output key 
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String[] mydata = value.toString().split("::");
				one.set("usr");
				usrid.set(mydata[0].trim());
				if(mydata[1].equals("F"))
				{
					context.write(usrid, one);
				}
				
			}
		}
		//The reducer class	for first step to do the join work
		public static class Reduce extends Reducer<Text,Text,Text,Text> 
		{
			private Text result = new Text();
			private Text myKey = new Text();
			private ArrayList<Text> list1 = new ArrayList<Text>(); //contain userid
			//contains uid and rating information pair
			//private HashMap<Text,ArrayList<Text>> list2 = new HashMap<Text,ArrayList<Text>>();
			private ArrayList<Text> list2 = new ArrayList<Text>();

			public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException
			{
				Boolean tag = false;
				String Key;
				String res;
				list1.clear();
				list2.clear();
				for(Text val : values)
				{
					if(val.toString().charAt(0) == 'u')
					{
						list1.add(new Text(val.toString().substring(1)));
					}
					else if(val.toString().charAt(0) == 'r')
					{
//						if(list2.containsKey(key))
//						{
//							list2.get(key).add(new Text(val.toString().substring(1)));
//						}
//						else 
//						{
//							list2.put(key, new ArrayList<Text>());
//							list2.get(key).add(new Text(val.toString().substring(1)));
//						}
						list2.add(new Text(val.toString().substring(1)));
					}
				}
				Iterator<Text> iter = list2.iterator();
				if(list1.size()!=0)
				{
					while(iter.hasNext())
					{
						Text temp = new Text();
						temp = iter.next();
						//String  v = temp.toString();
						String [] v = temp.toString().split("~");
						myKey.set(v[0]);
						result.set(v[1]);
						context.write(myKey, result);
						
					}
				}
				


			}
			
		}
			//next level mapreduce job
			//map the result of former job
			public static class Map3 extends Mapper<LongWritable, Text, Text, Text>
			{
				private Text mid = new Text();
				private Text rat = new Text();  // type of output key 
				public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
					String[] mydata = value.toString().split("\t");
					rat.set("r"+"\t"+mydata[1].trim());
					mid.set(mydata[0].trim());
					context.write(mid, rat);
					
				}
			}
		
			public static class Map4 extends Mapper<LongWritable, Text, Text, Text>
			{//take care of movie data
				private Text mid = new Text();
				private Text title = new Text();  // type of output key 
				public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
					String[] mydata = value.toString().split("::");
					title.set("t"+"\t"+mydata[1].trim());
					mid.set(mydata[0].trim());
					context.write(mid, title);
				}
			}
			//do second join to get movie name
			public static class Reduce2 extends Reducer<Text,Text,Text,Text> 
			{
				private Text result = new Text();
				private Text myKey = new Text();
				float avgrating;
				

				
				public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException
				{
					double sum = 0.0;
					double count = 0.0;
					String Key = null;
					String r =null;
					for (Text val : values) 
					{
						String [] k = val.toString().split("\t");
						if(k[0].equals("t"))
						{
							Key = k[1];
						}
						if(k[0].equals("r"))
						{
							
								count++;
								double s = Double.parseDouble(k[1]);
								sum+=s;
							
						}
						
					}
					if(sum != 0	)
					{
						Float avg = (float) (sum/count);
						r = String.valueOf(avg);
						//myKey.set(Key);
						//result.set(r);
						mymap.put(Key, r);
					}
						
			
				}
				
				protected void cleanup(Context context) throws IOException, InterruptedException
				{
					ArrayList<Map.Entry<String,String>> list=new ArrayList<Map.Entry<String,String>>(mymap.entrySet());
					Collections.sort(list,new Comparator<Map.Entry<String, String>>() {
						@Override
						public int compare(Entry<String, String> o1, Entry<String, String> o2) {
							//return o2.getValue().compareTo(o1.getValue()); //decreasing 
							if(Float.parseFloat(o1.getValue().toString())>Float.parseFloat(o2.getValue().toString()))
							{
								return 1;
							}
							else if(Float.parseFloat(o1.getValue().toString())==Float.parseFloat(o2.getValue().toString()))
							{
								return 0;
							}
							else
								return -1;
						}
					});
					Iterator<Map.Entry<String,String>> iter = mymap.entrySet().iterator();
				    while(iter.hasNext())
				    {
				    	//for(int i =0;i<5;i++)
				    	{
				    		Map.Entry<String, String> entry = iter.next();
				    		myKey.set( entry.getKey());
				    		result.set(entry.getValue());
				    		context.write(myKey, result);
				    	}
				    	//break;
				    }
				}
				
			}
		


	public static void main(String[] args)throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//		// get all args
//		if (otherArgs.length != 5) {
//			System.err.println("Usage: JoinExample <in> <in2> <in3> <out1> <out2>");
//			System.exit(2);
//		}
		Path ratIn = new Path(otherArgs[0]);
		Path userIn = new Path(otherArgs[1]);
		
		Path movIn = new Path(otherArgs[2]);
		
		Path out1 = new Path(otherArgs[3]);
		Path out2 = new Path(otherArgs[4]);
		FileSystem fs = FileSystem.get(conf);

		// create a job with name "fmovie" 
		Job job = new Job(conf, "join"); 
		job.setJarByClass(fmovie.class);
		job.setMapperClass(Map1.class);
		job.setMapperClass(Map2.class);
		
		job.setReducerClass(Reduce.class);

		// OPTIONAL :: uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);



		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class ,Map1.class );

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class,Map2.class );
		//MultipleInputs.addInputPath(job, new Path(otherArgs[2]),TextInputFormat.class );

		
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);

		//set the HDFS path of the input data
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		job.waitForCompletion(true);
		if(job.waitForCompletion(true))
		{
			Job top = new Job(conf, "get title"); 
			top.setJarByClass(fmovie.class);
			top.setMapperClass(Map3.class);
			top.setMapperClass(Map4.class);
			top.setReducerClass(Reduce2.class);

			// OPTIONAL :: uncomment the following line to add the Combiner
			// job.setCombinerClass(Reduce.class);

			


			MultipleInputs.addInputPath(top, new Path(otherArgs[3]), TextInputFormat.class ,Map3.class );

			MultipleInputs.addInputPath(top, new Path(otherArgs[2]),TextInputFormat.class,Map4.class );
			//MultipleInputs.addInputPath(job, new Path(otherArgs[2]),TextInputFormat.class );

			
			top.setOutputKeyClass(Text.class);
			// set output value type
			top.setOutputValueClass(Text.class);

			//set the HDFS path of the input data
			// set the HDFS path for the output 
			FileOutputFormat.setOutputPath(top, new Path(otherArgs[4]));

			top.waitForCompletion(true);
			//fs.delete(out1);

		}
	}

}
