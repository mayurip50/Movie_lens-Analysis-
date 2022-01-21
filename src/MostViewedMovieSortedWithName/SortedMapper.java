package MostViewedMovieSortedWithName;

import java.io.IOException;
import java.text.Format;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortedMapper extends Mapper<Object, Text, IntWritable, Text>{
	
public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
String line[] =value.toString().split("\\t");
String moviename=line[0].trim();


int count=0;
NumberFormat format=NumberFormat.getInstance(Locale.US);
Number number=null;
try {
	number=format.parse(line[1].trim());
count=Integer.parseInt(number.toString());
}
catch(ParseException e) {
	e.printStackTrace();
}

count=count*-1;

context.write(new IntWritable(count), new Text(moviename));
}
}
