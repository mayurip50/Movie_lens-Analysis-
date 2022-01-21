package GeneresRankedByRatings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class UserMapper extends Mapper<Object, Text, Text, Text> {
private String Age="";
private String Occupation="";
private String user_id="";

public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		String[] line=value.toString().split("::");

user_id=line[0].trim();
Occupation=line[3].trim();
		Path[] cacheFiles=new Path[0];
		Configuration conf=context.getConfiguration();
cacheFiles=DistributedCache.getLocalCacheFiles(conf);
BufferedReader br=new BufferedReader(new FileReader(cacheFiles[0].toString()));
String line1;
while((line1=br.readLine())!=null) {
	String codename[]=line1.split(":");
	String code=codename[0].trim();
	String occupation_name=codename[1].trim();
	if(Occupation.equalsIgnoreCase(code)) {
		Occupation=occupation_name;
		break;
	}
	
	
}

Integer age=Integer.parseInt(line[2].trim());

//18-35, 36-50 and 50+.
if(age>=18&&age<=35) {
	Age="18-35";
}
if(age>=36&&age<=50) {
	Age="36-50";
}
if(age>=50) {
	Age="50+";
}

context.write(new Text(user_id), new Text(Age+"::"+Occupation));

	}
}
