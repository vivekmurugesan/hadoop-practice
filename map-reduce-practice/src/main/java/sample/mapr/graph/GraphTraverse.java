package sample.mapr.graph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GraphTraverse {

	public static class GraphPathMapper extends Mapper<Object, Text, Text, Text>
	{
		/**
		 * n1\tn2,n3\tdist
		 */
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			String[] tokens =  value.toString().split("\t");
			String src = tokens[0];
			String neighbours = "";
			if(tokens.length > 1)
				neighbours = tokens[1];
			int dist = Integer.MAX_VALUE;
			if(tokens.length > 2)
				dist = Integer.parseInt(tokens[2]);
			
			int updatedDist = Integer.MAX_VALUE;
			if(dist < Integer.MAX_VALUE)
				updatedDist = dist + 1;
			Text out = new Text();
			out.set("DISTANCE\t"+updatedDist);
			if(neighbours != null && !neighbours.isEmpty()){
				for(String dest : neighbours.split(",")){
					context.write(new Text(dest), out);
				}
			}
			context.write(new Text(src), new Text("NODES\t"+neighbours+"\t"+dist));

		}
	}

	public static class GraphPathreducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			String neighbours = null;
			int distance = Integer.MAX_VALUE;
			
			for(Text t: values){
				String[] tokens = t.toString().split("\t");
				int currentDistance = Integer.MAX_VALUE;
				if("DISTANCE".equals(tokens[0]))
					currentDistance = Integer.parseInt(tokens[1]);
				else{
					currentDistance = Integer.parseInt(tokens[2]);
					neighbours = tokens[1];
				}
				if(currentDistance < distance)
					distance = currentDistance;
			}

			//Output a new node representation from the collected parts
			context.write(key, 
					new Text(neighbours+"\t" + distance));
		}
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception  {
		Configuration conf = new Configuration();
		if(args.length < 3){
			System.err.println("Usage GraphTraverse <input> <output> <iter_count>");
			System.exit(1);
		}
		int iterCount = Integer.parseInt(args[2]);
		for(int i=0;i<iterCount;i++){
			String input = args[0];
			String output = args[1]+"_"+(i+1);
			if(i>0){
				input = args[1]+"_"+i;
			}
			Job job = new Job(conf, "Graph-Traverse");
			/** job attribtues */
			job.setJarByClass(GraphTraverse.class);
			job.setMapperClass(GraphPathMapper.class);
			job.setReducerClass(GraphPathreducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			/** setting input and output*/
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			if(i < iterCount-1){
				System.out.printf("Iteration:%d triggered\n", i);
				job.waitForCompletion(true);
			}else{
				System.out.printf("Iteration:%d triggered and will be exiting post complettion!!\n",i);
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			}
				
		}
	}

}
