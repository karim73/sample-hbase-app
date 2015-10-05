package org.gelog.sys870;

import java.io.File;
import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class ZooKeeperWrapper implements Watcher
{
	static private final Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private ZooKeeper zkClient;
	private boolean   connected = false;
	
	
	public ZooKeeperWrapper( String connectionString, int sessionTimeout ) throws IOException
	{
		this.connect( connectionString, sessionTimeout );
	}
	
	
	public void connect( String connectionString, int sessionTimeout ) throws IOException
	{
		zkClient = new ZooKeeper( connectionString, sessionTimeout, this );
		
		// Wait for the connection to be established (the client connects asynchronously)
		try {
			int delay = 10;  // Check delay in milliseconds
			
			for( int i = 0; i <= sessionTimeout ; i += delay ) {
				if (connected)
					break;
				
				Thread.sleep( delay );
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	public void process(WatchedEvent zkEvent)
	{
		System.out.println( "\t" + zkEvent );
	}
	

	public void disconnect()
	{
		try {
			if (connected && zkClient != null)
				zkClient.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		zkClient  = null;
		connected = false;
	}
	
	
	public void list ( String path )
	{
		List<String>	children;
		String			childPath;
		int				dataLength;
		byte[]			data;
		Stat			stat = new Stat();
		long			modTime;
		String			modTimeStr;
		
		try {
			children	= zkClient.getChildren( path, false, stat );
			dataLength	= stat.getDataLength();
			modTime		= Math.max( stat.getMtime(), stat.getCtime() );
			modTimeStr  = ( modTime > 0  ?  format.format(new Date(modTime))  :  "                   ");
			
			if ( dataLength > 0 ) {
				data = zkClient.getData( path, false, stat );
				System.out.printf( "%7d %s %s = %s\n", dataLength, modTimeStr, path, data );
				//System.out.printf( "%7d %s %s\n", dataLength, modTimeStr, path );
			} else {
				System.out.printf( "%7d %s %s\n", dataLength, modTimeStr, path );
			}
			
			for (String child : children)
			{
				childPath = new File( path, child ).toString();
				
				this.list( childPath );
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
}
