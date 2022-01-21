package GeneresRankedByRatings;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgRatingReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{

	public void reduce(Text key,Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
double sum=0.0;
double avg=0.0;
double count=0.0;
		for(DoubleWritable val :values) {
			sum+=val.get();
		count++;
		}
		avg=sum/count;
		context.write(key, new DoubleWritable(avg));
	}
}
