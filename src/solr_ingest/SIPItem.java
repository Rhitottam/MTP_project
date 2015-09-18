package solr_ingest;

import java.io.File;

public class SIPItem {
	private File dublin_core;
	private File contents;
	private File item;
	private File meta_dcterms;
	private String parent;
	
	public void setDublin_core(File dublin_core){
		this.dublin_core = dublin_core;
	}
	public void setContents(File contents){
		this.contents = contents;
	}
	public void setItem(File item){
		this.item = item;
	}
	public void setMeta_dcterms(File meta_dcterms){
		this.meta_dcterms = meta_dcterms;
	}
	public void setParent(String parent){
		this.parent = parent;
	}
	
	
	
	public File getDublin_core(){
		return this.dublin_core;
	}
	public File getContents(){
		return this.contents;
	}
	public File getItem(){
		return this.item;
	}
	public File getMeta_dcterms(){
		return this.meta_dcterms;
	}
	public String getParent(){
		return this.parent;
	}
}
