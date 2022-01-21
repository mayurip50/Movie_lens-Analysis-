package GeneresRankedByRatings;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMovieRatingMapper extends Mapper<Object, Text, Text, Text>{
//	output of MyReducer	- context.write(new Text(uSerid), new Text(key+":"+Genre+":"+ratings));
public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
	String[] line=value.toString().split("\\t");
	String userid=line[0].trim();
	String multipleValues=line[1].trim();
	context.write(new Text(userid),new Text(multipleValues));
	
}
}
