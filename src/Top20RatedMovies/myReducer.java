package Top20RatedMovies;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer extends Reducer<Text, Text, Text, Text>{
	
public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	String moviename="";
	int Ratings=0;
	int total_ratings=0;
	double avg_rating=0.0;
	int count=0;
for(Text val :values) {
	String moviein=val.toString();
	if(moviein!=null) {
	if(moviein.endsWith(":1")) {
		String[] Splitted=moviein.split(":");
		Ratings=Integer.parseInt(Splitted[0]);
		total_ratings=total_ratings+Ratings;
		count++;
	}
	else {
		moviename=moviein;
	}
}
	}
if(count>=40) {
avg_rating=(double)total_ratings/(double)count;
context.write(new Text(moviename),new Text(""+avg_rating));
}
}	
}
