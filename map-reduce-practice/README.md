1. Run build with,
	>mvn clean install
2. Copy the jar from target dir into your hadoop master
3. Copy the graph-input.txt file into hdfs (from: src/main/resources location)
4. Run with the following cmd from your master
	>hadoop jar map-reduce-practice-0.0.1-SNAPSHOT.jar sample.mapr.graph.GraphTraverse graph-input.txt output 4
5. Verify the output generated wtih the following cmd,
	>hdfs dfs -cat output*/part*
