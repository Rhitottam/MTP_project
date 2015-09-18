package solr_ingest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.FileUtils;

public class SIPPackage {
	List<String> fileList;
    ///private static final String INPUT_ZIP_FILE = "C:\\MyFile.zip";
    //public String OUTPUT_FOLDER = "F:\\[Data]\\output";
    public String OUTPUT_FOLDER = ConfigManager.getProps().getProperty("output");
	//public String OUTPUT_FOLDER = new XMLConfiguration(System.getenv("SOLR_UPLOAD_ENV")).getString("output");
	public String INPUT_FOLDER;
    public String CONTENTS_FOLDER = ConfigManager.getProps().getProperty("contents");
    private File sip_zip;
    private String community_name;
    //private HashMap parent_map = new HashMap();
    private HashMap type_map = new HashMap();
    private MultiMap comm_name_path_map = new MultiValueMap();
	private String community_path;
	private String collection_name;
	private String root_dir;
    public SIPPackage(){
    	
    }
    public SIPPackage(File sip_zip,String community_path,String collection_name){
    	System.out.println(System.getProperty("user.dir"));
    	INPUT_FOLDER = sip_zip.getAbsolutePath().split("SIP")[0];
    	OUTPUT_FOLDER += sip_zip.getAbsolutePath().substring(sip_zip.getAbsolutePath().lastIndexOf(File.separator)).split(".zip")[0]; 
    	System.out.println(INPUT_FOLDER);
    	System.out.println(OUTPUT_FOLDER);
    	this.sip_zip = sip_zip;
    	this.community_path = community_path;
    	this.community_name = this.community_path;
    	this.collection_name = collection_name;
    	String[] communities = community_name.split("/");
    	String path = "";
    	//int count = 0;
    	for(int i=0;i<communities.length;i++){
    		if(i==0){
    			path+=communities[i];
    		}
    		else{
    			path+="/"+communities[i];
    		}
    		comm_name_path_map.put(communities[i],path);
    		
    	}
    }
    
    public MultiMap getCommNamePathMap(){
    	return this.comm_name_path_map;
    }
    public String getParentPath(){
    	/*String parent = "";
    	String child = "";
    	//String path = "";
    	for (String key : (Collection<String>)parent_map.keySet()){
    		parent = key ;
    		
    		while(parent_map.get(parent)!=null){
    			child = parent;
    			parent = (String) parent_map.get(parent);
    		}
    		//break;
    	}
    	System.out.println(child);
    	System.out.println((String) name_path_map.get(child));
    	return new File((String) name_path_map.get(child)).getParent();*/
    	return OUTPUT_FOLDER+File.separator+this.root_dir;
    		
    }
    public void uploadToContents(){
    	try {
    		File Contents_dir = new File(CONTENTS_FOLDER+File.separator+this.community_name.replace("/", File.separator)+File.separator+this.collection_name);
    		Contents_dir.setExecutable(true, false);
    		Contents_dir.setReadable(true, false);
    		Contents_dir.setWritable(true, false);
    		if(!Contents_dir.exists()){
    			Contents_dir.mkdirs();
    		}
			FileUtils.copyDirectory(new File(getParentPath()), new File(CONTENTS_FOLDER+File.separator+this.community_name.replace("/", File.separator)+File.separator+this.collection_name));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    public void unZipIt(){

        byte[] buffer = new byte[1024];
       	
        try{
       		
       	//create output directory is not exists
       	File folder = new File(OUTPUT_FOLDER);
       	if(!folder.exists()){
       		folder.mkdir();
       	}
       	folder.setExecutable(true,false);
       	folder.setReadable(true, false);
       	folder.setWritable(true, false);
       	//get the zip file content
       	ZipInputStream zis = 
       		new ZipInputStream(new FileInputStream(this.sip_zip.getAbsolutePath()));
       	//get the zipped file list entry
       	ZipEntry ze = zis.getNextEntry();
       	int dir_count=0;
       	this.root_dir="";
       	while(ze!=null){
       			
       	   String fileName = ze.getName();
       	   	  if(ze.isDirectory()&&dir_count==0){
       	   		  System.out.println("root: "+fileName);
       	   		  this.root_dir = fileName.substring(0,fileName.lastIndexOf("/"));
       	   		  
       	   		  //fileName = this.collection_name;
       	   	  }
              File newFile = new File(OUTPUT_FOLDER + File.separator + fileName);
                   
              System.out.println("file unzip : "+ newFile.getAbsoluteFile());
              //System.out.println("parent file unzip : "+ newFile.getParent().replace(root_dir,this.collection_name));     
               //create all non exists folders
               //else you will hit FileNotFoundException for compressed folder
              new File(newFile.getParent()).mkdirs();
              //new File(newFile.getParent().replace(root_dir,this.collection_name)).mkdirs();
              /*if(parent_map.get(newFile.getName())==null){
            	  parent_map.put(newFile.getName(), newFile.getParentFile().getName());
              }
              if(name_path_map.get(newFile.getName())==null){
            	  name_path_map.put(newFile.getName(), newFile.getAbsolutePath());
              }*/
              if(!ze.isDirectory()){
               
   	           FileOutputStream fos = new FileOutputStream(newFile);             
   	
   	           int len;
   	           while ((len = zis.read(buffer)) > 0) {
   	       		fos.write(buffer, 0, len);
   	           }
   	           if(ze.isDirectory()){
   	        	   dir_count++;
   	           }
   	           fos.close();
              }
              ze = zis.getNextEntry();
       	}
       	
           zis.closeEntry();
       	zis.close();
       	this.root_dir = this.root_dir.substring(0,this.root_dir.lastIndexOf("/"));
       	System.out.println("ROOT Directory : "+this.root_dir);	
       	System.out.println("Done");
       		
       }catch(IOException ex){
          ex.printStackTrace();
       }
      }
      
    /**
     * Unzip it
     * @param zipFile input zip file
     * @param output zip file output folder
     */
    
    public void unZipIt(String zipFile){

     byte[] buffer = new byte[1024];
    	
     try{
    		
    	//create output directory is not exists
    	File folder = new File(OUTPUT_FOLDER);
    	if(!folder.exists()){
    		folder.mkdir();
    	}
    		
    	//get the zip file content
    	ZipInputStream zis = 
    		new ZipInputStream(new FileInputStream(zipFile));
    	//get the zipped file list entry
    	ZipEntry ze = zis.getNextEntry();
    		
    	while(ze!=null){
    			
    	   String fileName = ze.getName();
    	   if(fileName.equalsIgnoreCase("sip")){
    		   fileName = this.collection_name;
    	   }
           File newFile = new File(OUTPUT_FOLDER + File.separator + fileName);
                
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
    
    public List<SIPItem> getItems(){
    	List<SIPItem> items = new ArrayList<SIPItem>() ;
    	listf(items);
    	return items;
    }
    public HashMap getTypeMap(){
    	//getCollectionType();
    	//System.out.println("debug33");
    	//getCommunityType();
    	//System.out.println("debug36");
    	return this.type_map;
    }
    public void getCommunityType(){
    	
    }
    /*
    public void getCommunityType(){
    	File directory = new File(getParentPath());

        // get all the files from a directory
        File[] fList = directory.listFiles();
        int flag = 0;
        for (File file : fList) {
            if (file.isFile()) {
            	String parent_node = new File(file.getParent()).getName();
            	if((type_map.get(parent_node)==null)||(((String)type_map.get(parent_node)).contains("COMMUNITY"))){
            		type_map.put(parent_node, "ITEM");
            	}
            	
          
            } else if (file.isDirectory()) {
            	String current_node = file.getName();
            	if(type_map.get(current_node)==null){
            		type_map.put(current_node, "COMMUNITY");
            	}
            	parent_map.put(current_node, this.community_name);
            	flag = 0;
                getCommunityType(file.getAbsolutePath());
            }
        }
    }
    public void getCommunityType(String directoryName){
    	File directory = new File(directoryName);

        // get all the files from a directory
        File[] fList = directory.listFiles();
        int flag = 0;
        for (File file : fList) {
            if (file.isFile()) {
            	String parent_node = new File(file.getParent()).getName();
            	if((type_map.get(parent_node)==null)||(((String)type_map.get(parent_node)).contains("COMMUNITY"))){
            		type_map.put(parent_node, "ITEM");
            	}
            	
          
            } else if (file.isDirectory()) {
            	String current_node = file.getName();
            	if(type_map.get(current_node)==null){
            		type_map.put(current_node, "COMMUNITY");
            	}
            	parent_map.put(current_node, this.community_name);
            	flag = 0;
                getCommunityType(file.getAbsolutePath());
            }
        }
    }
    public void getCollectionType(){
    	File directory = new File(getParentPath());
    	System.out.println("debug34");
        // get all the files from a directory
        File[] fList = directory.listFiles();
        int flag = 0;
        for (File file : fList) {
            if (file.isFile()) {
            	
            } else if (file.isDirectory()) {
            	String parent_node = new File(file.getParent()).getName();
            	
            	flag = 0;
                getCollectionType(file.getAbsolutePath());
            }
        }
    }
    public void getCollectionType(String directoryName){
    	File directory = new File(directoryName);

        // get all the files from a directory
        File[] fList = directory.listFiles();
        int flag = 0;
        
        for (File file : fList) {
            if (file.isFile()) {
            	String parent_node = new File(new File(directoryName).getParent()).getName();
            	if(type_map.get(parent_node)==null){
            		type_map.put(parent_node, "COLLECTION");
            	}
            	
            } else if (file.isDirectory()) {
            	String parent_node = new File(file.getParent()).getName();
            	
            	flag = 0;
                getCollectionType(file.getAbsolutePath());
            }
        }
    }*/
    
    
    public void listf(List<SIPItem> items) {
        File directory = new File(OUTPUT_FOLDER);

        // get all the files from a directory
        File[] fList = directory.listFiles();
        int flag = 0;
        SIPItem item = new SIPItem();
        for (File file : fList) {
            if (file.isFile()) {
            	flag = 1;
            	item.setParent(CONTENTS_FOLDER+File.separator+community_name.replace("/", File.separator)+File.separator+collection_name+File.separator+file.getParentFile().getName());
            	if(file.getName().contains("dublin_core.xml")){
            		item.setDublin_core(file);
            	}
            	else if(file.getName().contains("metadata_dcterms.xml")){
            		item.setMeta_dcterms(file);
            	}
            	else if(file.getName().contains("contents")){
            		item.setContents(file);
            	}
            	else if(!(file.getName().contains("_dumplog.log"))&&!(file.getName().contains("_dumptxt.txt"))){
            		item.setItem(file);
            	}
                //files.add(file);
            } else if (file.isDirectory()) {
            	flag = 0;
                listf(file.getAbsolutePath(),items);
            }
            
        }
        if(flag!=0){
        	items.add(item);
        }
   } 
    
    public void listf(String directoryName,List<SIPItem> items) {
        File directory = new File(directoryName);

        // get all the files from a directory
        File[] fList = directory.listFiles();
        int flag = 0;
        SIPItem item = new SIPItem();
        for (File file : fList) {
            if (file.isFile()) {
            	flag = 1;
            	item.setParent(CONTENTS_FOLDER+File.separator+community_name.replace("/", File.separator)+File.separator+collection_name+File.separator+file.getParentFile().getName());
            	
            	if(file.getName().contains("dublin_core.xml")){
            		item.setDublin_core(file);
            	}
            	else if(file.getName().contains("metadata_dcterms.xml")){
            		item.setMeta_dcterms(file);
            	}
            	else if(file.getName().contains("contents")){
            		item.setContents(file);
            	}
            	else if(!(file.getName().contains("_dumplog.log"))&&!(file.getName().contains("_dumptxt.txt"))&&!(file.getName().contains("handle"))){
            		item.setItem(file);
            	}
                //files.add(file);
            } else if (file.isDirectory()) {
            	flag = 0;
                listf(file.getAbsolutePath(),items);
            }
            
        }
        if(flag!=0){
        	items.add(item);
        }
   } 
    
    
    
}
