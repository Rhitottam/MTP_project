package solr_ingest;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.collections.MultiMap;
import org.postgresql.util.PSQLException;

public class PostgresDatabaseManager {
	private static Connection connection =  null;
	private static int items_table_flag = 0;
	private static int items_metadata_table_flag = 0;
	private static int communities_table_flag = 0;
	private static int collections_table_flag = 0;
	public static void connect(){
		System.out.println("-------- PostgreSQL "
				+ "JDBC Connection Testing ------------");
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Error "
					+ "PostGRESQL library not in your library path!");
			e.printStackTrace();
			return;
		}
		System.out.println("PostgreSQL JDBC Driver Registered!");		
		try {
			String db_name = ConfigManager.getProps().getProperty("db_name");
			String db_user = ConfigManager.getProps().getProperty("db_user");
			String db_pass = ConfigManager.getProps().getProperty("db_pass");
			connection = DriverManager.getConnection(
					"jdbc:postgresql://127.0.0.1:5432/"+db_name, db_user,
					db_pass);
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}
		if (connection != null) {
			if(items_table_flag==0){
				try{
					Statement stmnt = null;
					String sql = "DO $do$ BEGIN IF (SELECT EXISTS ("
							+ " SELECT 1"
							+ " FROM   pg_catalog.pg_class c"
							+ " JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
							+ " WHERE  n.nspname = 'public'"
							+ " AND    c.relname = 'items'"
							+ " AND    c.relkind = 'r'))= 'f' THEN";
					sql += " CREATE TABLE items"
							+ "(  id bigint NOT NULL,"
							+ "  last_indexed timestamp without time zone NOT NULL,"
							+ "  url text[] NOT NULL,"
							+ "  path text,"
							+ "  parent_collection text NOT NULL DEFAULT 'none'::text,"
							+ "  CONSTRAINT items_pkey PRIMARY KEY (id))"
							+ "WITH (OIDS=FALSE);";
					sql += " END IF; END $do$";
					stmnt = connection.createStatement();
					stmnt.execute(sql);
					stmnt.close();
					items_table_flag = 1;
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
			if(items_metadata_table_flag==0){
				try{
					Statement stmnt = null;
					String sql = "DO $do$ BEGIN IF (SELECT EXISTS ("
							+ " SELECT 1"
							+ " FROM   pg_catalog.pg_class c"
							+ " JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
							+ " WHERE  n.nspname = 'public'"
							+ " AND    c.relname = 'items_metadata'"
							+ " AND    c.relkind = 'r'))= 'f' THEN ";
					sql += "CREATE TABLE items_metadata"
							+ "(id bigint NOT NULL,"
							+ " last_indexed timestamp without time zone NOT NULL,"
							+ " CONSTRAINT items_metadata_pkey PRIMARY KEY (id)) "
							+ "WITH (OIDS=FALSE);";
					sql += " END IF; END $do$";
					stmnt = connection.createStatement();
					stmnt.execute(sql);
					stmnt.close();
					items_metadata_table_flag = 1;
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
			if(collections_table_flag==0){
				try{
					Statement stmnt = null;
					String sql = "DO $do$ BEGIN IF (SELECT EXISTS ("
							+ " SELECT 1"
							+ " FROM   pg_catalog.pg_class c"
							+ " JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
							+ " WHERE  n.nspname = 'public'"
							+ " AND    c.relname = 'collections'"
							+ " AND    c.relkind = 'r'))= 'f' THEN";
					sql += " CREATE TABLE collections"
							+ "( collection_id text NOT NULL,"
							+ "  collection_name text NOT NULL,"
							+ "  parent_community text NOT NULL,"
							+ "  CONSTRAINT collections_pkey PRIMARY KEY (collection_id))"
							+ "WITH (OIDS=FALSE);";
					sql += " END IF; END $do$";
					stmnt = connection.createStatement();
					stmnt.execute(sql);
					stmnt.close();
					collections_table_flag = 1;
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
			if(communities_table_flag==0){
				try{
					Statement stmnt = null;
					String sql = "DO $do$ BEGIN IF (SELECT EXISTS ("
							+ " SELECT 1"
							+ " FROM   pg_catalog.pg_class c"
							+ " JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
							+ " WHERE  n.nspname = 'public'"
							+ " AND    c.relname = 'communities'"
							+ " AND    c.relkind = 'r'))= 'f' THEN";
					sql += " CREATE TABLE communities"
							+ "( community_id text NOT NULL,"
							+ "  community_name text NOT NULL,"
							+ "  parent_community text NOT NULL,"
							+ "  CONSTRAINT communities_pkey PRIMARY KEY (community_id))"
							+ "WITH (OIDS=FALSE);";
					sql += " END IF; END $do$";
					stmnt = connection.createStatement();
					stmnt.execute(sql);
					stmnt.close();
					communities_table_flag = 1;
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
			
			
			
			//System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
		}
	}
	
	public static long check(String uri){
		Statement stmnt = null;
		long id = 0;
		try{
			String sql = "SELECT id FROM items WHERE '"+uri+"' = ANY(url);";
			stmnt = connection.createStatement();
			ResultSet rs = stmnt.executeQuery(sql);
			id = 0;
			while(rs.next()){
				id = rs.getLong("id");
			}
			rs.close();
			stmnt.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return id;
	}
	public static String getPath(String id){
		String path = "none";
		Statement stmnt = null;
		try{
			String sql = "SELECT path FROM items WHERE id = "+id+";";
			stmnt = connection.createStatement();
			ResultSet rs = stmnt.executeQuery(sql);
			path = "none";
			while(rs.next()){
				path = rs.getString("path");
			}
			rs.close();
			stmnt.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return path;
	}
	public static String getCommunities(){
		String path = "none";
		Statement stmnt = null;
		String communities = "[";
		try{
			String sql = "SELECT DISTINCT parent_community FROM communities";
			stmnt = connection.createStatement();
			ResultSet rs = stmnt.executeQuery(sql);
			path = "none";
			int count = 0;
			while(rs.next()){
				if(count==0){
					communities = communities+"\""+rs.getString("parent_community")+"\"";
				}
				else{
					communities = communities+",\""+rs.getString("parent_community")+"\"";
				}
				count++;
				//collections.add(rs.getString("collection_name"));
				//path = rs.getString("path");
			}
			
			rs.close();
			stmnt.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		communities = communities+"]";
		return communities;
		///return path;
	}
	public static String getCollections(){
		String path = "none";
		Statement stmnt = null;
		String collections = "[";
		try{
			String sql = "SELECT collection_name FROM collections";
			stmnt = connection.createStatement();
			ResultSet rs = stmnt.executeQuery(sql);
			path = "none";
			int count = 0;
			while(rs.next()){
				if(count==0){
					collections = collections+"\""+rs.getString("collection_name")+"\"";
				}
				else{
					collections = collections+",\""+rs.getString("collection_name")+"\"";
				}
				count++;
				//collections.add(rs.getString("collection_name"));
				//path = rs.getString("path");
			}
			
			rs.close();
			stmnt.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		collections = collections + "]";
		return collections;
		///return path;
	}
	public static void checkAndAddCollection(String collection, String id, String parent){
		
		Statement stmnt = null;
		try{
			connection.setAutoCommit(false);
			String sql = "DO $do$ BEGIN IF (SELECT COUNT(collection_id) "
					+ "FROM collections WHERE collection_name = '"+collection+"' AND parent_community = '"+parent+"')>0 "
					+ "THEN UPDATE collections SET collection_name = '"+collection+"' "
					+ ",parent_community = '"+parent+"' WHERE collection_id "
					+ "IN (SELECT collection_id FROM collections WHERE "
					+ "collection_name = '"+collection+"'); ELSE "
					+ "INSERT INTO collections(collection_id,collection_name,parent_community) "
					+ "VALUES('"+id+"','"+collection+"','"+parent+"'); END IF; END $do$";
			stmnt = connection.createStatement();
			stmnt.execute(sql);
			stmnt.close();
			connection.commit();
		
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	
	}
	public static void checkAndAddCommunity(String community, String id, String parent){
		
		Statement stmnt = null;
		try{
			connection.setAutoCommit(false);
			String sql = "DO $do$ BEGIN IF (SELECT COUNT(community_id) "
					+ "FROM communities WHERE community_name = '"+community+"' AND parent_community = '"+parent+"')>0 "
					+ "THEN UPDATE communities SET community_name = '"+community+"' "
					+ ",parent_community = '"+parent+"' WHERE community_id "
					+ "IN (SELECT community_id FROM communities WHERE "
					+ "community_name = '"+community+"'); ELSE "
					+ "INSERT INTO communities(community_id,community_name,parent_community) "
					+ "VALUES('"+id+"','"+community+"','"+parent+"'); END IF; END $do$";
			stmnt = connection.createStatement();
			stmnt.execute(sql);
			stmnt.close();
			connection.commit();
		
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	
	}
	public static void insertItem(long id,String path, Collection<String> uri, String parent){
		Statement stmnt = null;
		
		try{
			connection.setAutoCommit(false);
			Date dateTime = new Date(System.currentTimeMillis());
			String sql = "INSERT INTO items(id,last_indexed,url,path,parent_collection)"
					+ " VALUES("+id+",'"+new Timestamp(dateTime.getTime())+"','"+connection.createArrayOf("text", uri.toArray())+"','"+path+"','"+parent+"');";
			stmnt = connection.createStatement();
			System.out.println("Inserting item "+id+" .............");
			stmnt.executeUpdate(sql);
			stmnt.close();
			connection.commit();
			
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void updateItem(long id,String path, Collection<String> uri, String parent){
		Statement stmnt = null;
		
		try{
			connection.setAutoCommit(false);
			Date dateTime = new Date(System.currentTimeMillis());
			String sql = "UPDATE items SET last_indexed='"+new Timestamp(dateTime.getTime())+"',"
					+ " url='"+connection.createArrayOf("text", uri.toArray())+"',path='"+path+"',parent_collection='"+parent+"' WHERE id="+id+";";
			stmnt = connection.createStatement();
			System.out.println("Updating item "+id+" .............");
			stmnt.executeUpdate(sql);
			stmnt.close();
			connection.commit();
			
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void checkColumn(String col){
		Statement stmnt = null;
		int id = 0;
		try{
			String sql = "SELECT attname FROM pg_attribute "
		+ "WHERE attrelid = (SELECT oid FROM pg_class "
		+ "WHERE relname = 'items_metadata')AND attname = '"+col.toLowerCase().replace(".", "_")+"';";
			stmnt = connection.createStatement();
			//System.out.println(sql);
			ResultSet rs = stmnt.executeQuery(sql);
			id = 0;
			while(rs.next()){
				//System.out.println("Column Already present : "+rs.getString("attname"));
				id = 1;
			}
			if(id==0){
				System.out.println("Adding missing column........."+col);
				sql = "ALTER TABLE items_metadata ADD COLUMN "+col.toLowerCase().replace(".", "_")+" text[];";
				stmnt.execute(sql);
			}
			rs.close();
			stmnt.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void insertItem_metadata(long id,MultiMap item_metadata) throws SQLException{
		Statement stmnt = null;
		
		//try{
			connection.setAutoCommit(false);
			Date dateTime = new Date(System.currentTimeMillis());
			String sql = "INSERT INTO items_metadata(id,last_indexed";
			String values = " VALUES("+id+",'"+new Timestamp(dateTime.getTime())+"'";
			for(String mets:(Collection<String>)item_metadata.keySet()){
				if(!mets.equalsIgnoreCase("uri")){
					sql+=","+mets.replace(".", "_");
					values+=",'"+connection.createArrayOf("text", ((Collection<String>)item_metadata.get(mets)).toArray())+"'";
				}
			}
			sql=sql+") "+values+");";
			stmnt = connection.createStatement();
			System.out.println("Inserting item_metadata "+id+" .............");
			stmnt.executeUpdate(sql);
			stmnt.close();
			connection.commit();
			
			
		//}
		//catch(Exception e){
			//e.printStackTrace();
		//}
	}
	public static void updateItem_metadata(long id,MultiMap item_metadata){
		Statement stmnt = null;
		
		try{
			connection.setAutoCommit(false);
			Date dateTime = new Date(System.currentTimeMillis());
			String sql = "UPDATE items_metadata SET last_indexed='"+new Timestamp(dateTime.getTime())+"'";
			for(String mets:(Collection<String>)item_metadata.keySet()){
				if(!mets.equalsIgnoreCase("uri")){
					sql+=","+mets.replace(".", "_")+"='"+connection.createArrayOf("text", ((Collection<String>)item_metadata.get(mets)).toArray())+"'";
				}
			}
			sql+=" WHERE id="+id+";";
			System.out.println(sql);
			stmnt = connection.createStatement();
			System.out.println("Updating item_metadata "+id+" .............");
			stmnt.executeUpdate(sql);
			stmnt.close();
			connection.commit();
			
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void closeConnection(){
		if(connection!=null){
			try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
