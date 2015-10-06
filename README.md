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


