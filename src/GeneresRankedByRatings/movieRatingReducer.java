package GeneresRankedByRatings;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class movieRatingReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	// From Mappers - userMapper & UserMovieRatingMapper
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String genres = "genres";
		String movie_id = "movie_id";
		String age = "age";
		String occupation = "occupation";
		Double ratings = 0.0;
		List<Text> cache = new ArrayList<Text>();

		for (Text val : values) {
			String input = val.toString();
			if (input != null) {

				if (input.contains("::")) {
					// context.write(new Text(user_id), new Text(Age+"::"+Occupation));
					String[] SplitArray = input.split("::");
					age = SplitArray[0];
					occupation = SplitArray[1];
				} else {
					// context.write(new Text(uSerid), new Text(key+":"+Genre+":"+ratings));
					// context.write(new Text(userid),new Text(multipleValues));
					cache.add(val);
				}
			}
		} 
			  
			  for(Text val1 : cache) { 
				  String line1=val1.toString();
				if(line1!=null) {  
			  if(line1.contains(":")) { 
				  String[] array =line1.trim().split(":");
			  
			  genres=array[1]; 
			  NumberFormat _format =NumberFormat.getInstance(Locale.US); 
			  Number number = null; 
			  try { number =_format.parse(array[2].trim());
			  ratings =Double.parseDouble(number.toString()); } catch (ParseException e) {
				}
			  
			  //ratings=Double.parseDouble(array[2].toString());
			  //if(genres.contains("\\|")) { 
				  String[] genreArray=genres.split("\\|");
			  for(String gen:genreArray) { 
				  context.write(new Text(occupation+","+age+","+gen),new DoubleWritable(ratings)); 
				  } 
			  /*} 
			  else {
			  context.write(new Text(occupation+","+age+","+genres),new DoubleWritable(ratings)); }*/
			  
			  } 
			  }}
			 

		/*
		 * for (Text val1 :cache) { String currValue1 = val1.toString(); // check null
		 * condition if(currValue1 != null){ //if value contains : then it is
		 * movieRating mapper if(currValue1.contains(":")) { //split value with : String
		 * splitarray[] = currValue1.split(":"); // get the genres genres=
		 * splitarray[1].trim(); // get rating NumberFormat _format =
		 * NumberFormat.getInstance(Locale.US); Number number = null; try { number =
		 * _format.parse(splitarray[2].trim()); ratings =
		 * Double.parseDouble(number.toString()); } catch (ParseException e) {
		 * 
		 * } // split genres with | String splitarray1[] = genres.split("\\|"); int size
		 * = splitarray1.length; int i=0; for(i=0;i<size;i++){ // intermediate output
		 * will be movie_id and it's genres if(!genres.equalsIgnoreCase(""))
		 * context.write(new Text(occupation+","+age+","+splitarray1[i]), new
		 * DoubleWritable(ratings));
		 * 
		 * } // ignore data when genre is not available
		 * 
		 * }
		 * 
		 * 
		 * } }
		 */

	}
}