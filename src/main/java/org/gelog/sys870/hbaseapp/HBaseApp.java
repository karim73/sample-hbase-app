package org.gelog.sys870.hbaseapp;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HBaseApp
{
	private static final Logger LOG = LoggerFactory.getLogger(HBaseConfiguration.class);
	
	
    public static void main( String[] args ) throws IOException
    {
        System.out.println( "Hello World!" );
        
        new HBaseApp();
    }
    
    
    
    public HBaseApp() throws IOException
    {   
		Configuration		conf;
		Connection			conn;
    	Admin				admin;
    	String				tableName;
    	TableName			tableNameH;  // TableName used by HBase (bytes ?)
    	HTableDescriptor	table;
    	String				family;
    	InputStream			confStream;
    	
//    	Scanner reader = new Scanner(System.in);  // Reading from System.in
//    	System.out.println("Enter a number: ");
//    	int n = reader.nextInt();
    	
    	System.out.println("Setting up HBase configuration ...");
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

        
        ///conf.set("hbase.zookeeper.quorum", "localhost", "david");
        
        System.out.println("Connecting to HBase ZooKeeper Quorum ...");
        System.out.println("\t" + getPropertyTraceability(conf, "hbase.zookeeper.quorum") );
        System.out.println("\t" + getPropertyTraceability(conf, "hbase.zookeeper.property.clientPort") );
    	
        ZooKeeperWrapper	zk;
        String				zkConnectionString;
        int					zkSessionTimeout;
        
        zkConnectionString	= "192.168.99.100";
        zkSessionTimeout	= 3000;
        zk					= new ZooKeeperWrapper( zkConnectionString, zkSessionTimeout );
        
        System.out.println("Listing paths in ZooKeeper recursively ...");
        zk.list( "/" );
        
        zk.disconnect();
        //System.exit(1);
        
        
        conn		= ConnectionFactory.createConnection( conf );
    	
        tableNameH = TableName.valueOf("hbase:meta");
        
        
        
        
        System.exit(1);
        
        
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
    	
		// Add any necessary configuration files (hbase-site.xml, core-site.xml)
		//config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
		//config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
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
}
