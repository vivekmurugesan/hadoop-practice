package sample.mapr.graph;

import java.io.* ;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class GraphPath {
	
	public static class Node{
		// Id of the node
		private String id;
		// Ids of all the neighbors
		private String neighbours;
		// The distance of this node to the starting ndoe
		private int distance;
		// The current node state
		private String state;

		/** */
		Node(Text t){
			String[] parts = t.toString().split("\t") ;
			this.id = parts[0] ;
			this.neighbours = parts[1] ;
			if (parts.length<3 || parts[2].equals(""))
				this.distance = -1 ;
			else
				this.distance = Integer.parseInt(parts[2]) ;
			if (parts.length< 4 || parts[3].equals(""))
				this.state = "P" ;
			else
				this.state = parts[3] ;
		}
		// Create a node from a key and value object pair
		Node(Text key, Text value)
		{
			this(new Text(key.toString()+"\t"+value.toString())) ;
		}
		public String getId()
		{
			return this.id ;
		}
		public String getNeighbours()
		{
			return this.neighbours ;
		}
		public int getDistance()
		{
			return this.distance ;
		}
		public String getState()
		{
			return this.state ;
		}
	}
	
	public static class GraphPathMapper extends Mapper<Object, Text, Text, Text>
		 {
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			Node n = new Node(value);
			
			if(n.getState().equals("C")){
				// Output the node by changing its state to be done.
				context.write(new Text(n.getId()), new Text(
						n.getNeighbours()+"\t"+n.getDistance()+"\t"+"D"));
				/** Output each neighbour node by incrementing the distance by 1 */
				for(String neighbour : n.getNeighbours().split(",")){
					context.write(new Text(neighbour), 
							new Text("\t" + (n.getDistance()+1)+ "\tC"));
				}
			}else{
				// Output a pending node as is
				context.write(new Text(n.getId()), 
						new Text(n.getNeighbours() + "\t" + n.getDistance() + "\t" + n.getState()));
			}
				
		}
	}
	
	public static class GraphPathreducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
			String neighbours = null;
			int distance = -1;
			String state = "P";
			for(Text t: values){
				Node n = new Node(key, t);
				if(n.getState().equals("D")){
					// A done node should be the final output, ignore the remaining values
					neighbours = n.getNeighbours();
					distance = n.getDistance();
					state = n.getState();
					break;
				}
				// Select the list of neighbours when found
				if(n.getNeighbours() != null)
					neighbours = n.getNeighbours();
				// Select the largest distance 
				if(n.getDistance() > distance)
					distance = n.getDistance();
				// Select the highest remaining state
				if (n.getState().equals("D") ||
				(n.getState().equals("C") &&state.equals("P")))
					state=n.getState() ;
			}
			
			//Output a new node representation from the collected parts
			context.write(key, 
					new Text(neighbours+"\t" + distance +"\t" + state));
		}
	}
		

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception  {
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Graph-Path");
			/** job attribtues */
			job.setJarByClass(GraphPath.class);
			job.setMapperClass(GraphPathMapper.class);
			job.setReducerClass(GraphPathreducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			/** setting input and output*/
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}