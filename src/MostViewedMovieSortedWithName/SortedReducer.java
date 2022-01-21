package MostViewedMovieSortedWithName;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortedReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
private static int count;
@Override
public void setup(Context context) {
	Configuration conf=new Configuration();
	conf.setInt("count", 0);
}	
public void reduce(IntWritable key,Iterable<Text> value, Context context) throws IOException, InterruptedException {
	for(Text val :value) {
IntWritable key1= new IntWritable(-1*key.get());
if(count<10) {
	context.write(key1, new Text(val));
}
count++;
	}
}	
}
