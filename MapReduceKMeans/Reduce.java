package MapReduceKMeans;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;

//import SequentialKMeans.KMeans2.*;
import SequentialKMeans.KMeans2.SequentialKMeans.*;

/*
=========================================================================
I have to state here what the map function does
=========================================================================
*/

public class Reduce extends Reducer< IntWritable, Text, IntWritable, Text> {
//public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	 public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		 //ClustersCoordinatesSums sums = new ClustersCoordinatesSums();	
		 
		 double sumX=0.0, sumY=0.0;
		 int sumF=0;
		 Coordinates centroid = new Coordinates(0,0);
		 
		 //Read the coordinates point belonging to one cluster
		 for(Text val: values){
			 String currentLine = val.toString();
			CoordinatesSums Partialsum = new CoordinatesSums(currentLine);
			sumX+=Partialsum.X;
			sumY+=Partialsum.Y;
			sumF+=Partialsum.F;
		 }
		 
		 //Calculate the new centoid 
		 //I have to check here if divided by zero!!
		 
		 centroid.x = (int) (sumX/sumF);
		 centroid.y = (int) (sumY/sumF);
		 
		
		 //Write again the new centroid to the HDFS file
		 FileSystem fs = FileSystem.get(new Configuration());
		 Path path = new Path("/user/hadoop/NewCentroids.txt");

		 BufferedWriter meansWriter = new BufferedWriter(new OutputStreamWriter(fs.append(path)));

			
		 meansWriter.write(centroid.x + "," + centroid.y+"\n");
		 System.out.println(centroid.x + "," + centroid.y+"\n");
		 System.out.println("\n");
		 meansWriter.close();
		 
	 }
	 

}
