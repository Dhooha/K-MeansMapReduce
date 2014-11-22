package MapReduceKMeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.FileUtil;

import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;


import MapReduceKMeans.Map;
import MapReduceKMeans.Reduce;
import MapReduceKMeans.MapReduceKMeans;
import SequentialKMeans.KMeans2.SequentialKMeans;
import SequentialKMeans.KMeans2.SequentialKMeans.Constant;
import SequentialKMeans.KMeans2.SequentialKMeans.Coordinates;


public class MapReduceKMeans {
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        
		
        //i loop variable and Maximum number of iteration
		int i=0, thresoldLoop =80; //Counter and threshold for the main while loop
        
		
        //Define the threshold of the difference between the new centroid and the old centroid
		double difference=100, thresholdDifference = 1; //to measure the difference between the new and the old cluster
		
        
		Coordinates[] clusterOld = new Coordinates[Constant.NClusters]; //Old clusters
		//Initialize the old cluster to zero coordinates
		for(int d=0; d<Constant.NClusters; d++)
			clusterOld[d]= new Coordinates(0,0);
    
   
        
        
        
        //starting iterating the MapReduce jobs
        while(i < thresoldLoop && difference > thresholdDifference){
       
        //for(int i=1; i<4; i++){	
        
        Job job = new Job(conf, "KMeans");
        job.setJarByClass(MapReduceKMeans.class);
        
        //set the types expected as output from both the map and reduce phases.
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(Map.class);
        //job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        job.waitForCompletion(true);
        
           
        
        FileSystem srcFS = FileSystem.get(new Configuration());
		Path srcPath = new Path("/user/hadoop/NewCentroids.txt");
		if (!srcFS.exists(srcPath)) {
				System.out.println("The NewCentroids file is not found !!!");
				return;
			}
		 
		 FileSystem destFS = FileSystem.get(new Configuration());
		 Path destPath = new Path("/user/hadoop/InitialCentroids.txt");
		 if (!destFS.exists(destPath)) {
				System.out.println("The InitialCentroids file is not found !!!");
				return;
			}
		
		 
		 
		 //Read both of the files an store their values on cluster and clusterOld 
		 //I have to check if the NewCentroids file does not contain a new values that means the map and reduce function did not do their work
		 
		 Coordinates[] nSCentroid = new Coordinates[Constant.NClusters];
		 
		//Read the NewCentroids file containing the new centroids
		BufferedReader brNew = new BufferedReader(new InputStreamReader(srcFS.open(srcPath)));
		int iNew=0;
		String sNewCentroids;
		Coordinates[] newCentroid = new Coordinates[Constant.NClusters];
		while (iNew<Constant.NClusters && (sNewCentroids = brNew.readLine().trim()) != null) {
			nSCentroid[iNew] = new Coordinates(sNewCentroids);
			System.out.println("The non sorted centroid of the new index  " + iNew+ " "+ nSCentroid[iNew].x+ "," + nSCentroid[iNew].y);
			iNew++;
		}
		
		int varIndex =-1;
		//Sorting the results of the newCentroids
		for(int nNew=0; nNew<Constant.NClusters; nNew++)
		{
			Coordinates minNew = new Coordinates(100000, 100000);
			for(int mNew=0; mNew<Constant.NClusters; mNew++)
			{
				if(nSCentroid[mNew].x < minNew.x && nSCentroid[mNew].x !=-1 && nSCentroid[mNew].y != -1){
					varIndex = mNew;
					minNew.x = nSCentroid[mNew].x;
					minNew.y = nSCentroid[mNew].y;
					System.out.println("min new " + nNew+ " "+ minNew.x + "," + minNew.y );
				}
			}
			newCentroid[nNew] = new Coordinates(minNew.x, minNew.y);
			System.out.println("The new centroid index  " + nNew+ " "+ newCentroid[nNew].x+ "," + newCentroid[nNew].y);
			nSCentroid[varIndex].x = -1;
			nSCentroid[varIndex].y = -1;
		}
		
		 
		
		//Read the InitialCentroids file containing the old centroids
		BufferedReader brOld = new BufferedReader(new InputStreamReader(destFS.open(destPath)));
		int iOld=0;
		String sOldCentroids;
		Coordinates[] OldCentroid = new Coordinates[Constant.NClusters];
		while (iOld<Constant.NClusters && (sOldCentroids = brOld.readLine().trim()) != null) {
			nSCentroid[iOld] = new Coordinates(sOldCentroids);
			System.out.println("The non sorted of the old centroid index  " + iOld+ " "+ nSCentroid[iOld].x+ "," + nSCentroid[iOld].y);
			iOld++;
		}
		
		varIndex =-1;
		//Sorting the results of the oldCentroids
		for(int nOld=0; nOld<Constant.NClusters; nOld++)
		{
			Coordinates minOld = new Coordinates(100000, 100000);
			for(int mOld=0; mOld<Constant.NClusters; mOld++)
			{
				if(nSCentroid[mOld].x < minOld.x && nSCentroid[mOld].x !=-1 && nSCentroid[mOld].y != -1){
					varIndex = mOld;
					minOld.x = nSCentroid[mOld].x;
					minOld.y = nSCentroid[mOld].y;
					System.out.println("min old " + nOld+ " "+ minOld.x + "," + minOld.y );
				}
			}
			OldCentroid[nOld] = new Coordinates( minOld.x, minOld.y);
			System.out.println("The old centroid index  " + nOld + " "+ OldCentroid[nOld].x + "," + OldCentroid[nOld].y);
			nSCentroid[varIndex].x = -1;
			nSCentroid[varIndex].y = -1;
		}
		
		
		 //Calculate the difference between the new and the old centroid
			difference=0.0;
			for(int r=0; r<Constant.NClusters; r++)
				difference+= SequentialKMeans.EuclideanDistance(OldCentroid[r], newCentroid[r]);	
			System.out.println("the difference is " + difference);
			
		
		//copy the new centroid into the old one.
        if (!FileUtil.copy(srcFS, srcPath , destFS, destPath , true, true, new Configuration()))
        	System.out.println("copy is not working");
        
        BufferedWriter meansWriter = new BufferedWriter(new OutputStreamWriter(srcFS.create(srcPath,true)));
		meansWriter.close();
        
		System.out.println("done with the " + i + "job");
        i++;
        
        //I have to check if the new centroid are as the old ones or not.
        
        }
    }

}
