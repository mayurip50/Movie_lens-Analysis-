package MostViewedMovieSortedWithName;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer extends Reducer<Text, Text, Text, Text>{
	
public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	String moviename="";
	int Count=0;
for(Text val :values) {
	String moviein=val.toString();
	if(moviein!=null) {
	if(moviein.equalsIgnoreCase("1")) {
		Count++;
	}
	else {
		moviename=val.toString();
	}
}
	}


context.write(new Text(moviename),new Text(""+Count));

}	
}
