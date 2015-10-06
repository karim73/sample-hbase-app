package org.gelog.sys870;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Development process:
 * 
 * 1. Compile on your workstation
 * 		Using Maven : 
 * 
 * 		mvn package
 * 
 *      ls -lh target/*.jar
 *      -rw-r--r-- 1 david staff 8.4K Oct  4 23:29 target/hbase-app-0.0.1-SNAPSHOT.jar
 *		-rw-r--r-- 1 david staff  31M Oct  4 23:29 target/hbase-app-0.0.1-SNAPSHOT-fattyboy.jar
 *
 *		
 *		Using graddle :
 *		
 *		(eclipse usage) gradle eclipse 
 *		(idea usage) gradle idea
 *		
 *	    gradle package
 * 
 *      ls -lh build/libs/*.jar
 *		-rw-r--r-- 1 michael michael 37426474  5 oct 23:56 hbase-app-0.0.1-SNAPSHOT-all.jar
 * 
 * 2. Start a Docker container (using gradle, replace absolute path from target  to build/libs)
 * 		docker run --rm -ti \
 * 			-v $HOME/workspace/ets/sys870/sample-hbase-app/target:/opt/target \
 * 			hbase bash
 * 
 * 3. Run you job as follows (builded using gradle ? adapt jar name):
 * 		java -jar /opt/target/hbase-app-0.0.1-SNAPSHOT-fattyboy.jar
 * 
 * 
 * IMPROVEMENTS:
 *    FIXME: Normally, we should be able to run the non-fat jar as follows:
 *    
 *    java -cp .:$(hbase classpath) -jar hbase-app-0.0.1-SNAPSHOT.jar
 *    
 *    but it complains about not being able to find the Hadoop Configuration class.
 * 
 * 
 * TODO: The HBase ZooKeeper IP is hardcoded in src/main/resources.
 *       It would be great to find an elegant way to override this between
 *       local and dev HBase cluster.
 * 
 * TODO: Find a way to connect to Docker container from IDE.
 *       Currently, if I need hardcode the "hbase-master" IP address in /etc/hosts.
 *       It allows connecting to ZooKeeper, but it fails when connecting to HBase
 *       RegionServer. This seems to be because the RegionServer port is always
 *       changing when starting the HBase master, so it is impossible to NAT this
 *       port from the container through the Docker-Machine / Boot2Docker or 
 *       through a remote server.
 *       
 *       The port may be randomly assigned since HBase starts in non-distributed
 *       mode.
 * 
 * @author david
 * @see    https://github.com/larsgeorge/hbase-book
 *         for more Java API code examples.
 * @see    https://github.com/sleberknight/basic-hbase-examples
 */
public class HBaseApp
{
	private static final Logger LOG = LoggerFactory.getLogger(HBaseConfiguration.class);
	
	
    public static void main( String[] args ) throws IOException
    {
        new HBaseApp();
    }
    
    
    public HBaseApp() throws IOException
    {   
		Configuration		conf;
		Connection			conn;
    	String				zkConnectionString;
    	
//    	Scanner reader = new Scanner(System.in);  // Reading from System.in
//    	System.out.println("Enter a number: ");
//    	int n = reader.nextInt();
    	
    	System.out.println("Setting up HBase configuration ...");
    	conf = configureHBase();
        System.out.println("\t" + getPropertyTraceability(conf, "hbase.zookeeper.quorum") );
        System.out.println("\t" + getPropertyTraceability(conf, "hbase.zookeeper.property.clientPort") );

        
        // Note: Verify that the client can connect to ZooKeeper (normally not required)
        /*
        System.out.println("Connecting manually to ZooKeeper (not required) ...");
        zkConnectionString = conf.get( "hbase.zookeeper.quorum" );
        testingZooKeeper( zkConnectionString );
        */
        
        //System.exit(1);
        
        System.out.println("Using HBase client to connect to the ZooKeeper Quorum ...");
        conn	= ConnectionFactory.createConnection( conf );
    	locateTable( conn, "hbase:meta" );
    	locateTable( conn, "table1" );
    	locateTable( conn, "default:table1" );
    	
    	createTable( conn );
    	
        System.exit(1);
    }
    
    
    public Configuration configureHBase()
    {
    	Configuration		conf;
    	InputStream			confStream;
    	
    	conf		= HBaseConfiguration.create();
    	confStream  = conf.getConfResourceAsInputStream("hello.xml");
        int available = 0;
        try {
            available = confStream.available();
        } catch (Exception e) {
            //for debug purpose
        	System.out.println("configuration files not found locally");
        } finally {
            IOUtils.closeQuietly( confStream );
        }
        if (available == 0 ) {
            conf = new Configuration();
            conf.addResource("core-site.xml");
            conf.addResource("hbase-site.xml");
            conf.addResource("hdfs-site.xml");
        }

		// Add any necessary configuration files (hbase-site.xml, core-site.xml)
		//config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
		//config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        
        conf.set("zookeeper.session.timeout", "100");
        
        return conf;
    }
    
    
    public String getPropertyTraceability( Configuration conf, String key )
    {
        String		value;
        String[]	sources;
        String		source;
        
        value	= conf.get( key );
        sources = conf.getPropertySources( key );
        // Only keep the most recent source (last in the array)
        source	= (sources != null  ?  sources[sources.length-1]  :  "");
        
        return key + " = " + value + " (" + source + ")";
    }
    
    
    public void testingZooKeeper( String zkConnectionString ) throws IOException
    {
    	ZooKeeperWrapper	zk;
        int					zkSessionTimeout;
        
        zkSessionTimeout	= 3000;
        zk					= new ZooKeeperWrapper( zkConnectionString, zkSessionTimeout );
        
        System.out.println("Listing paths in ZooKeeper recursively ...");
        zk.list( "/" );
        
        zk.disconnect();
    }
    
    
    public void locateTable( Connection connection, String tableName ) throws IOException
    {
    	TableName	tableNameH;  // TableName used by HBase (bytes ?)
    	byte[]		startRow;
    	String		host;
    	
    	List<HRegionLocation>	locations;
    	HRegionInfo				regionInfo;
    	
    	System.out.println("Locating the '" + tableName + "' table ...");
    	
    	tableNameH	= TableName.valueOf( tableName );
    	startRow	= null;
    	locations	= connection.getRegionLocator(tableNameH).getAllRegionLocations();
    	
    	for (HRegionLocation location : locations) {
			host		= location.getHostnamePort();
			regionInfo	= location.getRegionInfo();
			startRow	= regionInfo.getStartKey();
			
			System.out.printf("\tHost: %s has region #%d starting at row %s\n", 
					host, 
					regionInfo.getRegionId(), 
					startRow );
			
			System.out.println( );
		}
    }
    
    
    public void createTable( Connection conn ) throws IOException
    {
    	Admin				admin;
    	String				tableName;
    	TableName			tableNameH;  // TableName used by HBase (bytes ?)
    	HTableDescriptor	table;
    	String				family;
    	
    	admin		= conn.getAdmin();
    	
    	tableName	= "demo-table";
    	tableNameH	= TableName.valueOf( tableName );
		
    	if ( admin.tableExists(tableNameH) ) {
    		System.out.println("Table already exists. Deleting table " + tableName);
    		admin.disableTable( tableNameH );
    		admin.deleteTable( tableNameH );
    	}
    	
    	System.out.println("Creating table " + tableName);
    	family		= "cf";
    	table		= new HTableDescriptor( tableNameH );
    	table.addFamily( new HColumnDescriptor( family ) );
    	admin.createTable( table );
    }
}
