package MapReduceKMeans;

import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import SequentialKMeans.KMeans2.*;
import SequentialKMeans.KMeans2.SequentialKMeans.*;


//I have to see the difference between LongWritable and InWritable
public class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	//Declaration and Initialization of the initial clusters
	public static Coordinates[] centroid = new Coordinates[Constant.NClusters];
	
	
	 @Override
	  protected void setup(Context context) throws IOException, InterruptedException {
	    super.setup(context);
	    
	    	//Read the centroids from the HDFS file
	  		FileSystem fs = FileSystem.get(new Configuration());
	  		Path path = new Path("/user/hadoop/InitialCentroids.txt");
	  		if (!fs.exists(path)) {
	  			System.out.println("Means does not exist !!!");
	  			return;
	  		}
	  		
	  		//Variable to read on it the line content
	  		String sCurrentLine;
	  		
	  		//Read the HDFS file containing the centroids
	  		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
	  		int i=0;
	  		while (i<Constant.NClusters && (sCurrentLine = br.readLine().trim()) != null) {
	  			//System.out.println(sCurrentLine);
	  			//filling the centroid array coordinates from the HDFS file
	  			centroid[i] = new Coordinates(sCurrentLine);
	  			//System.out.println(centroid[i].x +":" + centroid[i].y);
	  			i++;
	  		}
	  		
	  		//System.out.println("after loop");
	  		//Close the File
	  		if (br != null)
	  			br.close();
	  		//System.out.println("after close");
	  		
	    
	 }
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException{
		
		//Read the coordinates sensor point, and store the value on the Coordinates variable
		Coordinates point = new Coordinates(value.toString());
		
		//Declaration and Initialization of the initial clusters 
		//Coordinates[] centroid = new Coordinates[Constant.NClusters];
		
		

		
		//distance is a variable issued from the Distance class to store 
		//the calculated distances between var and each centroid 
		Distance distance = new Distance();
		//System.out.println("after creating distance");
		for(int p=0; p<Constant.NClusters; p++){
			distance.dist[p]= SequentialKMeans.EuclideanDistance(centroid[p], point);
			//System.out.println(distance.dist[p]);
		}
		
		//Calculate the nearest centroid for the point, and store its index in the index variable 
		int index=distance.CalculClusterIndex();
		
	
		
		
		
		//Preparing the emission of the results to the reducer
		String toSend = new String();
		toSend=value.toString() + ",1";
		
		System.out.println("key: " + index + "    value:" + toSend);
		//Send the results to the Reducer
		//The result would have the form key=clusterindex value= sensor coordinates in a string form (Text in the case of Hadoop)
		try {
			context.write(new IntWritable(index), new Text(toSend));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
