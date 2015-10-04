package org.gelog.sys870.hbaseapp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;


public class HBaseApp 
{
	
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
    	
    	System.out.println("Connecting to HBase ...");
    	conf		= HBaseConfiguration.create();
    	conn		= ConnectionFactory.createConnection( conf );
    	admin		= conn.getAdmin();
    	
    	tableName	= "demo";
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
}
