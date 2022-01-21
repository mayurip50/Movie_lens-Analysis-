package GeneresRankedByRatings;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgRatingMapper extends Mapper<Object, Text, Text, DoubleWritable>{

	public void map(Object key,Text value,Context context) throws IOException, InterruptedException
	{
	//from reducer - MovieRatingReducer context.write(new Text(occupation+","+age+","+gen),new DoubleWritable(ratings));

		String[] splitarray=value.toString().split("\\t");
		String multiplevalues=splitarray[0];
	Double ratings=Double.parseDouble(splitarray[1]);
	
	context.write(new Text(multiplevalues),new DoubleWritable(ratings));
	
	}
}
