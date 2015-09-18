package solr_ingest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class AIPPackage {
	public String OUTPUT_FOLDER = ConfigManager.getProps().getProperty("output");;
    private String INPUT_FOLDER;
    private File aip_tar;
    private HashMap title_map = new HashMap();
    private HashMap parent_map = new HashMap();
    private HashMap parent_path_map = new HashMap();
    private HashMap type_map = new HashMap();
    private HashMap items_metadata = new HashMap();
    public AIPPackage(){
    	
    }
    public HashMap getTitleMap(){
    	return this.title_map;
    }
    public HashMap getParentMap(){
    	return this.parent_map;
    }
    public HashMap getTypeMap(){
    	return this.type_map;
    }
    public HashMap getItemsMetadata(){
    	return this.items_metadata;
    }
    
	public AIPPackage(File aip_tar){
		INPUT_FOLDER = aip_tar.getAbsolutePath().substring(0,aip_tar.getAbsolutePath().lastIndexOf(File.separator));
    	//OUTPUT_FOLDER += aip_tar.getAbsolutePath().substring(aip_tar.getAbsolutePath().lastIndexOf(File.separator)).split(".zip")[0]; 
		OUTPUT_FOLDER+=File.separator+aip_tar.getName().split("___aip___")[0];
		System.out.println(INPUT_FOLDER);
    	System.out.println(OUTPUT_FOLDER);
    	this.aip_tar = aip_tar;
    	System.out.println("debug 1");
    	File archive = aip_tar;
		File destination = new File(OUTPUT_FOLDER);
		System.out.println("debug 2");
		Archiver archiver = ArchiverFactory.createArchiver("tar", "gz");
		try {
			archiver.extract(archive, destination);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("debug 3");
		OUTPUT_FOLDER += File.separator+aip_tar.getName().split("___aip___")[1].split(".tar")[0];
		extract_zip(OUTPUT_FOLDER);
		System.out.println("debug 4");
    	
	}
	public void unZipIt(File zipFile){

	     byte[] buffer = new byte[1024];
	    	
	     try{
	    		
	    	//create output directory is not exists
	    	File folder = new File(zipFile.getParent()+File.separator+zipFile.getName().replace(".zip", ""));
	    	if(!folder.exists()){
	    		folder.mkdir();
	    	}
	    		
	    	//get the zip file content
	    	ZipInputStream zis = 
	    		new ZipInputStream(new FileInputStream(zipFile.getAbsolutePath()));
	    	//get the zipped file list entry
	    	ZipEntry ze = zis.getNextEntry();
	    		
	    	while(ze!=null){
	    			
	    	   String fileName = ze.getName();
	           File newFile = new File(zipFile.getAbsolutePath().replace(".zip", "") + File.separator + fileName);
	                
	           System.out.println("file unzip : "+ newFile.getAbsoluteFile());
	           System.out.println("parent file unzip : "+ newFile.getParent().replace("\\", "/")+"/");     
	            //create all non exists folders
	            //else you will hit FileNotFoundException for compressed folder
	            
	           new File(newFile.getParent()).mkdirs();
	           if(!ze.isDirectory()){   
		           FileOutputStream fos = new FileOutputStream(newFile);             
		
		           int len;
		           while ((len = zis.read(buffer)) > 0) {
		       		fos.write(buffer, 0, len);
		           }
		        		
		           fos.close();
	           }
	           ze = zis.getNextEntry();
	    	}
	    	
	        zis.closeEntry();
	    	zis.close();
	    		
	    	System.out.println("Done");
	    		
	    }catch(IOException ex){
	       ex.printStackTrace(); 
	    }
	   }
	public void extract_zip(String directoryName) {
       File directory = new File(directoryName);

       // get all the files from a directory
       File[] fList = directory.listFiles();
       int flag = 0;
       //SIPItem item = new SIPItem();
       for (File file : fList) {
           if (file.isFile()) {
           	flag = 1;
           	if(file.getName().endsWith(".zip")){
           		unZipIt(file);
           		if(file.delete()){
           			System.out.println(file.getName()+" deleted..");
           		}
           		else{
           			System.out.println("not able to delete "+file.getName());
           		}
           	}
               //files.add(file);
           } else if (file.isDirectory()) {
           	flag = 0;
               extract_zip(file.getAbsolutePath());
           }
           
       }
       if(flag!=0){
       	//items.add(item);
       }
	}
	public static Element get_dmdSec(NodeList nList){
		for(int i=0;i<nList.getLength();i++){
			Element item  = (Element)nList.item(i);
			if(item.getAttribute("ID").equalsIgnoreCase("dmdSec_2")){
				return item;
			}
		}
		return (Element)nList.item(0);
	}
	public void get_details(){
		File directory = new File(OUTPUT_FOLDER);
		File[] fList = directory.listFiles();
	       int flag = 0;
	       //SIPItem item = new SIPItem();
	       for (File file : fList) {
	           if (file.isFile()) {
	           	flag = 1;
	           	if(file.getName().equalsIgnoreCase("mets.xml")){
	           		try {	
	           			String hdl_id = "" ;
	           			String type = "";
	           			String parent_hdl_id = "";
	           			String title = "";
	           			MultiMap item_metadata = new MultiValueMap();
	           			DocumentBuilderFactory dbFactory  = DocumentBuilderFactory.newInstance();
	           			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	           			Document doc = dBuilder.parse(file);
	           			doc.getDocumentElement().normalize();
	           			NodeList nList = doc.getElementsByTagName("mets");
	           			
	           			//System.out.println("----------------------------");
	           			for (int temp = 0; temp < nList.getLength(); temp++) {
	           				Node nNode = nList.item(temp);
	           				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
	           					Element eElement = (Element)nNode;
	           					if(eElement.getAttribute("OBJID")!= null){
	           						//metadata_name+=eElement.getAttribute("element");
	           						hdl_id = eElement.getAttribute("OBJID");
	           						System.out.println("Handle ID: "+hdl_id);
	           					}
	           					if(eElement.getAttribute("TYPE")!=null){
	           						//metadata_name+="_"+eElement.getAttribute("qualifier");
	           						type = eElement.getAttribute("TYPE");
	           						System.out.println("Type: "+type);
	           					}
	           					
	           				}


	           			}
	           			NodeList source_info = ((Element)((Element)((Element)((Element)doc.getElementsByTagName("sourceMD").item(0)).getElementsByTagName("mdWrap").item(0)).getElementsByTagName("xmlData").item(0)).getElementsByTagName("dim:dim").item(0)).getElementsByTagName("dim:field");
	           			for (int i = 0;i<source_info.getLength();i++){
	           				Element eElement = (Element)source_info.item(i);
	           				if(eElement.getAttribute("element").equalsIgnoreCase("relation")&&eElement.getAttribute("qualifier").equalsIgnoreCase("isPartOf")){
	           					parent_hdl_id = eElement.getTextContent();
	           					System.out.println("Parent: "+parent_hdl_id);
	           				}
	           				//if()
	           			}
	           			NodeList metadata = ((Element)((Element)((Element)(get_dmdSec(doc.getElementsByTagName("dmdSec")).getElementsByTagName("mdWrap").item(0))).getElementsByTagName("xmlData").item(0)).getElementsByTagName("dim:dim").item(0)).getElementsByTagName("dim:field");
	           			for (int i = 0;i<metadata.getLength();i++){
	           				Element eElement = (Element)metadata.item(i);
	           				if(eElement.getAttribute("element").equalsIgnoreCase("title")){
	           					title = eElement.getTextContent();
	           					System.out.println("Title: "+title);
	           				}
	           				
	           				//if()
	           			}
	           			title_map.put(hdl_id, title);
	           			parent_map.put(hdl_id, parent_hdl_id);
	           			type_map.put(hdl_id, type);
	           			
	           			if(type.contains("ITEM")){
	           				for (int i = 0;i<metadata.getLength();i++){
		           				Element eElement = (Element)metadata.item(i);
		           				String metadata_name = "";
		           				
		           				if((eElement.getAttribute("element") != null)||(!eElement.getAttribute("element").equalsIgnoreCase(""))){
		           					metadata_name+=eElement.getAttribute("element");
		           				}
		           				if((eElement.getAttribute("qualifier") != null)||(!(eElement.getAttribute("qualifier").equalsIgnoreCase("")))){
		           					metadata_name+="."+eElement.getAttribute("qualifier");
		           				}
		           				if((eElement.getAttribute("mdschema")!=null)||(!eElement.getAttribute("mdschema").equalsIgnoreCase(""))){
		           					metadata_name = eElement.getAttribute("mdschema") + "." + metadata_name;
		           				}
		           				//if(eElement.)
		           				item_metadata.put(metadata_name,removeFormattingCharacters(eElement.getTextContent()));
		           				if(metadata_name.contains("identifier.uri")){
		           					item_metadata.put("uri", eElement.getTextContent());
		           				}
		           				System.out.println("Metadata "+metadata_name+": "+eElement.getTextContent());
		           				//if()
		           			}
	           			}
	           			
	           			
	           		}catch (Exception e) {
	           			e.printStackTrace();
	           		}
	           	}
	               //files.add(file);
	           } else if (file.isDirectory()) {
	           	flag = 0;
	               get_details(file.getAbsolutePath());
	           }
	           
	       }
	       if(flag!=0){
	       	//items.add(item);
	       }
	}
	public HashMap getParentPathMap(){
		for(String item : ((Collection<String>)this.parent_map.keySet())){
			String parent_path = "";
			if(((String)type_map.get(item)).contains("ITEM")){
				parent_path = "";
			}
			else{
				parent_path = (String) title_map.get(item);
			}
			
			String parent = item;
			while(true){
				if(title_map.get(parent_map.get(parent))!=null){
					parent_path = (String)title_map.get(parent_map.get(parent))+ "/" + parent_path;
				}
				else{
					break;
				}
				parent = (String) parent_map.get(parent);
			}
			parent_path_map.put(item, parent_path);
		}
		return parent_path_map;
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
	public void get_details(String directoryName){
		File directory = new File(directoryName);
		File[] fList = directory.listFiles();
	       int flag = 0;
	       //SIPItem item = new SIPItem();
	       for (File file : fList) {
	           if (file.isFile()) {
	           	flag = 1;
	           	if(file.getName().equalsIgnoreCase("mets.xml")){
	           		try {	
	           			String hdl_id = "" ;
	           			String type = "";
	           			String parent_hdl_id = "";
	           			String title = "";
	           			MultiMap item_metadata = new MultiValueMap();
	           			DocumentBuilderFactory dbFactory  = DocumentBuilderFactory.newInstance();
	           			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	           			Document doc = dBuilder.parse(file);
	           			doc.getDocumentElement().normalize();
	           			NodeList nList = doc.getElementsByTagName("mets");
	           			
	           			//System.out.println("----------------------------");
	           			for (int temp = 0; temp < nList.getLength(); temp++) {
	           				Node nNode = nList.item(temp);
	           				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
	           					Element eElement = (Element)nNode;
	           					if(eElement.getAttribute("OBJID")!= null){
	           						//metadata_name+=eElement.getAttribute("element");
	           						hdl_id = eElement.getAttribute("OBJID");
	           						System.out.println("Handle ID: "+hdl_id);
	           					}
	           					if(eElement.getAttribute("TYPE")!=null){
	           						//metadata_name+="_"+eElement.getAttribute("qualifier");
	           						type = eElement.getAttribute("TYPE");
	           						System.out.println("Type: "+type);
	           					}
	           					
	           				}


	           			}
	           			NodeList source_info = ((Element)((Element)((Element)((Element)doc.getElementsByTagName("sourceMD").item(0)).getElementsByTagName("mdWrap").item(0)).getElementsByTagName("xmlData").item(0)).getElementsByTagName("dim:dim").item(0)).getElementsByTagName("dim:field");
	           			for (int i = 0;i<source_info.getLength();i++){
	           				Element eElement = (Element)source_info.item(i);
	           				if(eElement.getAttribute("element").equalsIgnoreCase("relation")&&eElement.getAttribute("qualifier").equalsIgnoreCase("isPartOf")){
	           					parent_hdl_id = eElement.getTextContent();
	           					System.out.println("Parent: "+parent_hdl_id);
	           				}
	           				//if()
	           			}
	           			NodeList metadata = ((Element)((Element)((Element)(get_dmdSec(doc.getElementsByTagName("dmdSec")).getElementsByTagName("mdWrap").item(0))).getElementsByTagName("xmlData").item(0)).getElementsByTagName("dim:dim").item(0)).getElementsByTagName("dim:field");
	           			for (int i = 0;i<metadata.getLength();i++){
	           				Element eElement = (Element)metadata.item(i);
	           				if(eElement.getAttribute("element").equalsIgnoreCase("title")){
	           					title = eElement.getTextContent();
	           				}
	           				//if()
	           			}
	           			title_map.put(hdl_id, title);
	           			parent_map.put(hdl_id, parent_hdl_id);
	           			type_map.put(hdl_id, type);
	           			
	           			if(type.contains("ITEM")){
	           				for (int i = 0;i<metadata.getLength();i++){
		           				Element eElement = (Element)metadata.item(i);
		           				String metadata_name = "";
		           				if((eElement.getAttribute("element") != null)||(!eElement.getAttribute("element").equalsIgnoreCase(""))){
		           					metadata_name+=eElement.getAttribute("element");
		           				}
		           				if((eElement.getAttribute("qualifier") != null)||(!(eElement.getAttribute("qualifier").equalsIgnoreCase("")))){
		           					metadata_name+="."+eElement.getAttribute("qualifier");
		           				}
		           				if((eElement.getAttribute("mdschema")!=null)||(!eElement.getAttribute("mdschema").equalsIgnoreCase(""))){
		           					metadata_name = eElement.getAttribute("mdschema") + "." + metadata_name;
		           				}
		           				//if(eElement.)
		           				item_metadata.put(metadata_name,removeFormattingCharacters(eElement.getTextContent()));
		           				if(metadata_name.contains("identifier.uri")){
		           					item_metadata.put("uri", eElement.getTextContent());
		           				}
		           				System.out.println("Metadata "+metadata_name+": "+eElement.getTextContent());
		           				//if()
		           			}
	           				items_metadata.put(hdl_id, item_metadata);
	           			}
	           			
	           		}catch (Exception e) {
	           			e.printStackTrace();
	           		}
	           	}
	               //files.add(file);
	           } else if (file.isDirectory()) {
	           	flag = 0;
	               get_details(file.getAbsolutePath());
	           }
	           
	       }
	       if(flag!=0){
	       	//items.add(item);
	       }
	}
	
}
