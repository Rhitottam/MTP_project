package solr_ingest;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.SystemMenuBar;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.poi.hwpf.model.ListFormatOverrideLevel;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ContentHandler;

public class Ingest {
	public static HttpSolrServer _server = new HttpSolrServer(ConfigManager.getProps().getProperty("solr_collection_path"));
	public static String serverBaseAddress = ConfigManager.getProps().getProperty("download_base_path");
	public static AutoDetectParser _autoparser;
	public static void _server_config(){
		_server.setMaxRetries(1);
		_server.setSoTimeout(100000);
		_server.setConnectionTimeout(1000);
		_server.setParser(new XMLResponseParser());
		_autoparser = new AutoDetectParser();
		
	}
	
	
	public static String removeFormattingCharacters(final String toBeEscaped) {
        StringBuffer escapedBuffer = new StringBuffer();
        for (int i = 0; i < toBeEscaped.length(); i++) {
            if ((toBeEscaped.charAt(i) != '\n') && (toBeEscaped.charAt(i) != '\r') && (toBeEscaped.charAt(i) != '\t')) {
                escapedBuffer.append(toBeEscaped.charAt(i));
            }
        }
        String s = escapedBuffer.toString().replace("'", "`").replace("\"", "`");
        return s;//
        // Strings.replaceSubString(s, "\"", "")
    }
	public static long getIdentifier(){
		long id = System.currentTimeMillis();
		Random rnd = new Random();
		id = id*1000 + 100 +rnd.nextInt(900);
		return id;
	}
	public static String getCollectionIdentifier(){
		String id = "COll" +  new Long(System.currentTimeMillis()).toString();
		Random rnd = new Random();
		id = id + new Long(1000 + rnd.nextInt(9000)).toString();
		return id;
	}
	public static String getCommunitityIdentifier(){
		String id = "COMM" +  new Long(System.currentTimeMillis()).toString();
		Random rnd = new Random();
		id = id + new Long(10000 + rnd.nextInt(90000)).toString();
		return id;
	}
	private static void dump_txt(String text, File item){
		String filename = item.getAbsolutePath().substring(0,item.getAbsolutePath().lastIndexOf(File.separator))
				+File.separator+item.getName().substring(0,item.getName().lastIndexOf("."))+"_dumptxt.txt";
		System.out.println("Content Text file : "+filename);
		try{
			File content_txt = new File(filename);
			if(!content_txt.exists()){
				content_txt.createNewFile();
			}
			FileWriter fw  = new FileWriter(content_txt.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(text);
			bw.close();
			
			//out.w
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void dump_metadata(Metadata metadata, File item){
		String filename = item.getAbsolutePath().substring(0,item.getAbsolutePath().lastIndexOf(File.separator))
				+File.separator+item.getName().substring(0,item.getName().lastIndexOf("."))+"_dumplog.log";
		System.out.println("Metadata file : "+filename);
		try{
			File content_txt = new File(filename);
			if(!content_txt.exists()){
				content_txt.createNewFile();
			}
			FileWriter fw  = new FileWriter(content_txt.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			for (String name:metadata.names()){
				bw.write(name+" : "+metadata.get(name)+"\n");
			}
			
			bw.close();
			
			//out.w
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void uploadToSolr_batch(Collection solr_items){
		try{
			if (solr_items.size() >= 10) {
		          // Commit within 10 seconds.
		        UpdateResponse resp = _server.add(solr_items, 10000);
		        if (resp.getStatus() != 0) {
		          System.out.println("Error!!! , status : " +
		                  resp.getStatus());
		        }
		        solr_items.clear();
		      }
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void uploadToSolr(Collection solr_items){
		try{
			if (solr_items.size() > 0) {
		          // Commit within 10 seconds.
		        UpdateResponse resp = _server.add(solr_items, 10000);
		        if (resp.getStatus() != 0) {
		          System.out.println("Error!!! , status : " +
		                  resp.getStatus());
		        }
		        solr_items.clear();
		      }
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void ingest_sip(File sip_zip, String community_name, String collection_name){
		_server_config();
		System.out.println(community_name);
		System.out.println(collection_name);
		SIPPackage sip_package = new SIPPackage(sip_zip,community_name,collection_name);
		sip_package.unZipIt();
		File parent_det_file = new File("parent_det.txt");
		try {
			parent_det_file.createNewFile();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			FileWriter fw  = new FileWriter(parent_det_file);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(sip_package.getCommNamePathMap().toString());
			bw.close();
			fw.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		Collection<SIPItem> items = new ArrayList();
		try{
		PostgresDatabaseManager.connect();
		items = sip_package.getItems();
		System.out.println("debug1");
		MultiMap parent_map = sip_package.getCommNamePathMap();
		System.out.println("debug2");
		HashMap type_map = sip_package.getTypeMap();
		System.out.println("debug3");
		for(String comm: (Collection<String>)parent_map.keySet()){
			for (String comm_parent:(Collection<String>)parent_map.get(comm) ){
				PostgresDatabaseManager.checkAndAddCommunity(comm, getCommunitityIdentifier(), comm_parent);
			}
		}
		PostgresDatabaseManager.checkAndAddCollection(collection_name, getCollectionIdentifier(), community_name+"/"+collection_name);
		/*
		for(String key :(Collection<String>)type_map.keySet() ){
			if(((String)type_map.get(key)).contains("COMMUNITY")){
				//insert_collection
				PostgresDatabaseManager.checkAndAddCommunity(key, getCommunitityIdentifier(), (String)parent_map.get(key));
				
			}
			else if(((String)type_map.get(key)).contains("COLLECTION")){
				//insert community
				PostgresDatabaseManager.checkAndAddCollection(key, getCollectionIdentifier(), (String)parent_map.get(key));
				
			}
		}		
		
		*/
		System.out.println("debug3");
		}
		catch(Exception e){
			e.printStackTrace();
		}
		Collection solr_items = new ArrayList();
		
	
		for(SIPItem item:items){
			SolrInputDocument solr_item = new SolrInputDocument();
			if(item.getContents()!=null){
				System.out.println("Contents: "+item.getContents().getName());
			}
			if(item.getDublin_core()!=null){
				System.out.println("Dublin Core: "+item.getDublin_core().getName());
			}
			if(item.getMeta_dcterms()!=null){
				System.out.println("Meta DCterms: "+item.getMeta_dcterms().getName());
			}
			if(item.getItem()!=null){
				System.out.println("Item: "+item.getItem().getName());
			}
			MultiMap item_metadata = new MultiValueMap();
			long id = 0;
			String path = "none";
			try {	
		         
		         DocumentBuilderFactory dbFactory  = DocumentBuilderFactory.newInstance();
		         DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		         Document doc = dBuilder.parse(item.getDublin_core());
		         doc.getDocumentElement().normalize();
		         NodeList nList = doc.getElementsByTagName("dcvalue");
		         
		         System.out.println("----------------------------");
		         for (int temp = 0; temp < nList.getLength(); temp++) {
		            Node nNode = nList.item(temp);
		            String metadata_name = "";
		            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
		            	Element eElement = (Element)nNode;
		            	if(eElement.getAttribute("element")!= null){
		            		metadata_name+=eElement.getAttribute("element");
		            	}
		            	if(eElement.getAttribute("qualifier")!=null){
		            		metadata_name+="."+eElement.getAttribute("qualifier");
		            	}
		            	metadata_name = "dc." + metadata_name;
		            	if(metadata_name!=""){
		            		PostgresDatabaseManager.checkColumn(metadata_name);
			            	solr_item.addField(metadata_name, removeFormattingCharacters(eElement.getTextContent()));
		            		item_metadata.put(metadata_name, removeFormattingCharacters(eElement.getTextContent()));
			            }
		            	if(eElement.getAttribute("qualifier").equalsIgnoreCase("uri")&&eElement.getAttribute("element").equalsIgnoreCase("identifier")){
		            		System.out.println("URL : "+removeFormattingCharacters(eElement.getTextContent()));
		            		solr_item.addField("uri", removeFormattingCharacters(eElement.getTextContent()));
		            		item_metadata.put("uri",removeFormattingCharacters(eElement.getTextContent()));
		            	}
		            }
		            
		            
		         }
		     }catch (Exception e) {
		         e.printStackTrace();
		     }
			try{
				if(item.getItem()!=null){
					ContentHandler _text_handler = new BodyContentHandler(); 
					Metadata _metadata = new Metadata();
					ParseContext context = new ParseContext();
					InputStream input = new FileInputStream(item.getItem());
					_autoparser.parse(input, _text_handler, _metadata, context);
					dump_txt(_text_handler.toString(),item.getItem());
					dump_metadata(_metadata,item.getItem());
					solr_item.addField("fulltext", _text_handler.toString());
				}
				
			}
			catch(Exception e){
				e.printStackTrace();
			}
			try{
			Collection<String> mets = item_metadata.keySet();
			for(String met:mets){
				//System.out.println(met + " : " + Arrays.toString(((Collection<String>) item_metadata.get(met)).toArray()));
			}
			for(String url_item:(Collection<String>)item_metadata.get("uri")){
				if(PostgresDatabaseManager.check(url_item)!=0){
					
					id = PostgresDatabaseManager.check(url_item);
					break;
				}
				
			}
			if(item.getItem()!=null){
				path = item.getItem().getAbsolutePath().replace(sip_package.getParentPath(), sip_package.CONTENTS_FOLDER+File.separator+"Community");
			}
			if(id==0){
				id = getIdentifier();
				if(item.getItem()!=null){
					item_metadata.put("uri",serverBaseAddress+"/download/"+id);
					path = item.getParent()+File.separator+item.getItem().getName();
				}
				System.out.println("Item "+id+" to be inserted..........");
				try{
					PostgresDatabaseManager.insertItem(id, path, (Collection<String>)item_metadata.get("uri"), community_name+"/"+collection_name+"/"+id);
					PostgresDatabaseManager.insertItem_metadata(id, item_metadata);
				}catch(Exception e){
					break;
				}
				
			}
			else{
				if(item.getItem()!=null){
					item_metadata.put("uri",serverBaseAddress+"/download/"+id);
					path = item.getParent()+File.separator+item.getItem().getName();
				}
				System.out.println("Item "+id+" to be updated..........");
				try{
					PostgresDatabaseManager.updateItem(id, path, (Collection<String>)item_metadata.get("uri"), community_name+"/"+collection_name+"/"+id);
					PostgresDatabaseManager.updateItem_metadata(id, item_metadata);
				}catch(Exception e){
					break;
				}
			}
			solr_item.addField("search.uniqueid", new Long(id).toString());
			solr_items.add(solr_item);
			}
			catch(Exception e){
				e.printStackTrace();
			}
			uploadToSolr_batch(solr_items);
			//break;
		}
		
		uploadToSolr(solr_items);
		sip_package.uploadToContents();
		PostgresDatabaseManager.closeConnection();
	}
	
	public static void ingest_aip(File aip_file){
		AIPPackage aip = new AIPPackage(aip_file);
		aip.get_details();
		File det_file = new File("det.txt");
		try {
			det_file.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			FileWriter fw = new FileWriter(det_file);
			
			BufferedWriter bw = new BufferedWriter(fw);
			//bw.write(aip.title_map.toString());
			//bw.write(aip.type_map.toString());
			//bw.write(aip.parent_map.toString());
			int count = 0;
			for(String index : (Collection<String>)aip.getItemsMetadata().keySet()){
				bw.write("Item "+index+"\n"+"-----------------------\n");
				bw.write(aip.getItemsMetadata().get(index).toString()+"\n\n");
				count++;
			}
			bw.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HashMap type_map = aip.getTypeMap();
		HashMap title_map = aip.getTitleMap();
		HashMap parent_map = aip.getParentMap();
		HashMap parent_path_map = aip.getParentPathMap();
		HashMap items_metadata = aip.getItemsMetadata();
		PostgresDatabaseManager.connect();
		Collection solr_items = new ArrayList();
		for(String index : (Collection<String>)type_map.keySet()){
			if(type_map.get(index).toString().contains("COLLECTION")){
				PostgresDatabaseManager.checkAndAddCollection(title_map.get(index).toString(), getCollectionIdentifier(), parent_path_map.get(index).toString());
			}
			else if(type_map.get(index).toString().contains("COMMUNITY")){
				if(title_map.get(parent_map.get(index))!=null){
					//System.out.println(parent_map.get(key));
					PostgresDatabaseManager.checkAndAddCommunity(title_map.get(index).toString(), getCommunitityIdentifier(), parent_path_map.get(index).toString());
				}
				else{
					PostgresDatabaseManager.checkAndAddCommunity(title_map.get(index).toString(), getCommunitityIdentifier(), parent_path_map.get(index).toString());
				}
			}
			else if(type_map.get(index).toString().contains("ITEM")){
				long item_id = 0;
				SolrInputDocument solr_item = new SolrInputDocument();
				for(String item_url:(Collection<String>)((MultiMap)items_metadata.get(index)).get("uri")){
					item_id = PostgresDatabaseManager.check(item_url);
					if(item_id!=0){
						break;
					}
				}
				
				for(String key:(Collection<String>)((MultiMap)items_metadata.get(index)).keySet()){
					PostgresDatabaseManager.checkColumn(key);
					for (String value:(Collection<String>)((MultiMap)(items_metadata.get(index))).get(key)){
						if(!key.equalsIgnoreCase("uri")){
							solr_item.addField(key, value);
						}
						else{
							solr_item.addField(key, value);
						}
					}
				}
				
				
				if(item_id==0){
					item_id = getIdentifier();
					PostgresDatabaseManager.insertItem(item_id, "none", (Collection<String>)((MultiMap)items_metadata.get(index)).get("uri"),parent_path_map.get(index)+"/"+item_id);
					
					try {
						PostgresDatabaseManager.insertItem_metadata(item_id, (MultiMap)items_metadata.get(index));
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						break;
					}
				}
				else{
					PostgresDatabaseManager.updateItem(item_id, "none", (Collection<String>)((MultiMap)items_metadata.get(index)).get("uri"),parent_path_map.get(index)+"/"+item_id);
					
					try {
						PostgresDatabaseManager.updateItem_metadata(item_id, (MultiMap)items_metadata.get(index));
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				solr_item.addField("search.uniqueid",item_id);
				solr_items.add(solr_item);
				uploadToSolr_batch(solr_items);
			}
			uploadToSolr(solr_items);
			//bw.write("Item "+index+"\n"+"-----------------------\n");
			//bw.write(aip.getItemsMetadata().get(index).toString()+"\n\n");
			//count++;
		}
		PostgresDatabaseManager.closeConnection();
	}
}
