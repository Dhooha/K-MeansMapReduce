package MapReduceKMeans;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import SequentialKMeans.KMeans2.SequentialKMeans.CoordinatesSums;

public class Combine extends Reducer< IntWritable, Text, IntWritable, Text> {
	public void combine(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
	
		 double sumX=0.0, sumY=0.0;
		 int sumF=0;
		 
		 
		 //Read the coordinates point belonging to one cluster
		 for(Text val: values){
			 String currentLine = val.toString();
			CoordinatesSums Partialsum = new CoordinatesSums(currentLine);
			sumX+=Partialsum.X;
			sumY+=Partialsum.Y;
			sumF+=Partialsum.F;
		 }
		 FileSystem fs = FileSystem.get(new Configuration());
		 Path path = new Path("/user/hadoop/initialClustersCoordinates.txt");
		 
		 BufferedWriter meansWriter = new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
		 meansWriter.close();
		
		 System.out.println("after creation file combiner");
		 
		 String toSend = new String();
		 toSend= sumX + "," + sumY + "," + sumF;
		 try {
				context.write(key, new Text(toSend));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
}
	 