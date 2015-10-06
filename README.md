# Sample HBase App
Sample Java application that communicates with HBase via the HBase API.


## Maven instructions 

1) Compile on your workstation
```
mvn package
 
ls -lh target/*.jar
-rw-r--r-- 1 david staff 8.4K Oct  4 23:29 target/hbase-app-0.0.1-SNAPSHOT.jar
-rw-r--r-- 1 david staff  31M Oct  4 23:29 target/hbase-app-0.0.1-SNAPSHOT-all.jar
```

2) Start a Docker container
```
docker run --rm -ti \
	-v $HOME/workspace/ets/sys870/sample-hbase-app/target:/opt/target \
	hbase bash
```

3) Run you job as follows
```
java -jar /opt/target/hbase-app-0.0.1-SNAPSHOT-all.jar
```


## Gradle instructions

0) Generate the project for import into your IDE
- For Eclipse: `gradle eclipse`
- For IDEA: `gradle idea`

1) Compile on your workstation
```
gradle package
 
ls -lh build/libs/*.jar
-rw-r--r-- 1 michael michael 37426474  5 oct 23:56 hbase-app-0.0.1-SNAPSHOT-all.jar
```

2) Start a Docker container
```
docker run --rm -ti \
	-v $HOME/workspace/ets/sys870/sample-hbase-app/target:/opt/target \
	hbase bash
```

3) Run you job as follows
```
java -jar /opt/target/hbase-app-0.0.1-SNAPSHOT-all.jar
```


## Improvements

    FIXME: Normally, we should be able to run the non-fat jar as follows:
    
    `java -cp .:$(hbase classpath) -jar hbase-app-0.0.1-SNAPSHOT.jar`
    
    but it complains about not being able to find the Hadoop Configuration class.
 
 
 TODO: The HBase ZooKeeper IP is hardcoded in src/main/resources.
       It would be great to find an elegant way to override this between
       local and dev HBase cluster.
 
 TODO: Find a way to connect to Docker container from IDE.
       Currently, if I need hardcode the "hbase-master" IP address in /etc/hosts.
       It allows connecting to ZooKeeper, but it fails when connecting to HBase
       RegionServer. This seems to be because the RegionServer port is always
       changing when starting the HBase master, so it is impossible to NAT this
       port from the container through the Docker-Machine / Boot2Docker or 
       through a remote server.
       
       The port may be randomly assigned since HBase starts in non-distributed
       mode.

