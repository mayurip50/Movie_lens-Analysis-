package GeneresRankedByRatings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer extends Reducer<Text, Text, Text, Text>{
	//from mappers - movieMapper & ratingsMapper 
public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	
	String Genre="";
	List<String> cache=new ArrayList<String>(); 
	for(Text val :values) {
		String nextvalue=val.toString();
		if(!nextvalue.contains(":")) {
			Genre=nextvalue;
		}
		else {
			cache.add(nextvalue);
		}
	}
	
	for(String C :cache ) {
		Double ratings=0.0;
		String uSerid="";
		if(C!=null) {
		String tokens[]=C.split(":");
		uSerid=tokens[0];
		ratings=Double.parseDouble(tokens[1]);
		
		context.write(new Text(uSerid), new Text(key+":"+Genre+":"+ratings));
		
		}
	}

}	
}
