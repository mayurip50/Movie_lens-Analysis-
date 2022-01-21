package Top20RatedMovies;

import java.io.IOException;
import java.text.Format;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortedMapper extends Mapper<Object, Text, DoubleWritable, Text>{
	
public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
String line[] =value.toString().split("\\t");
String moviename=line[0].trim();


Double ratings=0.0;
NumberFormat format=NumberFormat.getInstance(Locale.US);
Number number=null;
try {
	number=format.parse(line[1].trim());
ratings=Double.parseDouble(number.toString());
}
catch(ParseException e) {
	e.printStackTrace();
}

ratings=ratings*-1;

context.write(new DoubleWritable(ratings), new Text(moviename));
}
}
