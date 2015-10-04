package org.gelog.sys870.hbaseapp;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
    	
    	Scanner reader = new Scanner(System.in);  // Reading from System.in
    	System.out.println("Enter a number: ");
    	int n = reader.nextInt();
    	
    	
    	LOG.info("Setting up HBase configuration ...");
    	conf		= HBaseConfiguration.create();
    	confStream  = conf.getConfResourceAsInputStream("hello.xml");
        int available = 0;
        try {
            available = confStream.available();
        } catch (Exception e) {
            //for debug purpose
            LOG.debug("configuration files not found locally");
        } finally {
            IOUtils.closeQuietly( confStream );
        }
        if (available == 0 ) {
            conf = new Configuration();
            conf.addResource("core-site.xml");
            conf.addResource("hbase-site.xml");
            conf.addResource("hdfs-site.xml");
        }
        
        //conf.get(name);
        
    	//if (!new File("hello.xml").exists())
    	//	throw new IOException("file not exsist");
    	
    	
    	//conf.addResource("blablabla.com");
    	//conf.addResource("hbase-site.xml");

    	
    	LOG.info("Connecting to HBase ...");
    	conn		= ConnectionFactory.createConnection( conf );
    	admin		= conn.getAdmin();
    	
    	
    	tableName	= "demo-table";
    	tableNameH	= TableName.valueOf( tableName );
		
    	if ( admin.tableExists(tableNameH) ) {
    		System.out.println("Table already exists. Deleting table " + tableName);
    		admin.disableTable( tableNameH );
    		admin.deleteTable( tableNameH );
    	}
    	
    	LOG.info("Creating table " + tableName);
    	family		= "cf";
    	table		= new HTableDescriptor( tableNameH );
    	table.addFamily( new HColumnDescriptor( family ) );
    	admin.createTable( table );
    	
		// Add any necessary configuration files (hbase-site.xml, core-site.xml)
		//config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
		//config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
    }
}
