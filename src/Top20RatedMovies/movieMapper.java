package Top20RatedMovies;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class movieMapper extends Mapper<LongWritable,Text, Text,Text>{

public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException 
{
String tokens[]=value.toString().split("::");
	String movieid=tokens[0].trim();
	String movieName=tokens[1].trim();
context.write(new Text(movieid),new Text(movieName));	
}	
}
