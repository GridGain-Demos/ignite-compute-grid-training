# Apache Ignite Compute Engine Demo

The demo contains several simple applications that demonstrate how to use IgniteCompute API.

In order to start any examples, the cluster has to be created and activated via any tool.

Use [https://github.com/antkr/ignite-compute-demo/blob/main/src/main/java/org/gridgain/demo/Server.java](https://github.com/antkr/ignite-compute-demo/blob/main/src/main/java/org/gridgain/demo/Server.java) to start a simple server or https://github.com/antkr/ignite-compute-demo/blob/main/src/main/java/org/gridgain/demo/ServerJobStealing.java to run JobStealing example.

# Run the server docker containers

1. Build project and docker image that will run the demo client:
   >sh ./rebuild-app-image.sh
2. Run the server nodes and scale them using docker-compose:
   >docker-compose -f docker/ignite-cluster.yaml up -d --scale ignite-server-node=2
3. Run the docker image that will start a container with demo client:
   >docker-compose -f docker/ignite-compute-app.yaml up -d
4. Navigate to the ignite-streaming-app console:
   - If you are using Linux, run:
   > 
   >docker exec -it ignite-compute-app bash
   > 
   - If you are using Windows, run:
   > 
   >winpty docker exec -it ignite-compute-app bash
5. Run the example based on the classname:
   >java -cp ./ignite-compute-app.jar:./dependency-jars/* org.gridgain.demo.compute.IgniteMapReduce
