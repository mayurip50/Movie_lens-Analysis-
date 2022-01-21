package GeneresRankedByRatings;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.GenericOptionsParser;


public class MyDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
Configuration conf=new Configuration();
String otherArgs[]=new GenericOptionsParser(conf,args).getRemainingArgs();
Job job=new Job(conf,"movie lens");

job.setJarByClass(MyDriver.class);
		/*
		 * job.setMapperClass(movieMapper.class);
		 * job.setMapperClass(ratingsMapper.class);
		 */
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	DistributedCache.addFileToClassPath(new Path("/user/mayuri/movielens/CodeOccupation.txt"), conf);
	//job.setReducerClass(myReducer.class);
	
	MultipleInputs.addInputPath(job, new Path(otherArgs[0]),TextInputFormat.class, movieMapper.class);
	MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, ratingsMapper.class);
	
	job.setReducerClass(myReducer.class);
	
FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
//System.exit(job.waitForCompletion(true)?0:1);
job.waitForCompletion(true);

Job job1=job.getInstance(conf,"Avg rating");


job1.setJarByClass(MyDriver.class);
//job1.setMapperClass(UserMapper.class);
job1.setReducerClass(movieRatingReducer.class);


job1.setMapOutputKeyClass(Text.class);
job1.setMapOutputValueClass(Text.class);

job1.setOutputKeyClass(Text.class);
job1.setOutputValueClass(DoubleWritable.class);

MultipleInputs.addInputPath(job1, new Path(otherArgs[3]), TextInputFormat.class,UserMapper.class);
MultipleInputs.addInputPath(job1, new Path(otherArgs[2]), TextInputFormat.class, UserMovieRatingMapper.class);

FileOutputFormat.setOutputPath(job1, new Path(otherArgs[4]));
job1.waitForCompletion(true);
	
Job job2 = Job.getInstance(conf, "Avg Rating2");
		//set Driver class
		job2.setJarByClass(MyDriver.class);
		//set Mapper class
		job2.setMapperClass(AvgRatingMapper.class);
		//set Reducer class
		job2.setReducerClass(AvgRatingReducer.class);

		//output format for mapper
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(DoubleWritable.class);

      //output format for reducer
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job2, new Path(otherArgs[4]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5]));

		job2.waitForCompletion(true);

	}

}
