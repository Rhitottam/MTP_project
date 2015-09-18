<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="java.io.*,java.util.*, javax.servlet.*" %>
<%@ page import="javax.servlet.http.*" %>
<%@ page import="org.apache.commons.fileupload.*" %>
<%@ page import="org.apache.commons.fileupload.disk.*" %>
<%@ page import="org.apache.commons.fileupload.servlet.*" %>
<%@ page import="org.apache.commons.io.output.*" %>
<%@ page import="solr_ingest.*" %>

	<%
	File file ;
	   int maxFileSize = 5 * 1024 * 1024 * 1024;
	   int maxMemSize = 5 * 1024 * 1024 * 1024;
	   ServletContext context = pageContext.getServletContext();
	   String filePath = context.getInitParameter("file-upload");

	   // Verify the content type
	   String contentType = request.getContentType();
	   if ((contentType.indexOf("multipart/form-data") >= 0)) {

	      DiskFileItemFactory factory = new DiskFileItemFactory();
	      // maximum size that will be stored in memory
	      factory.setSizeThreshold(maxMemSize);
	      // Location to save data that is larger than maxMemSize.
	      factory.setRepository(new File("F:\\temp_upload"));

	      // Create a new file upload handler
	      ServletFileUpload upload = new ServletFileUpload(factory);
	      // maximum file size to be uploaded.
	      upload.setSizeMax( maxFileSize );
	      try{ 
	         // Parse the request to get file items.
	         List fileItems = upload.parseRequest(request);

	         // Process the uploaded file items
	         Iterator i = fileItems.iterator();

	         String community_path = null;
	         String collection_name = null;
	         file = null;
	         while ( i.hasNext () ) 
	         {
	            FileItem fi = (FileItem)i.next();
	           
	            if(fi.isFormField()){
	            	
	            	String fieldName = fi.getFieldName();
	            	System.out.println(fieldName);
	            	String fieldValue = fi.getString();
	            	if(fieldName.equalsIgnoreCase("community_name")){
	            		community_path = fieldValue;
	            	}
	            	if(fieldName.equalsIgnoreCase("collection_name")){
	            		collection_name = fieldValue;
	            	}
	            }
	            if ( !fi.isFormField () )	
	            {
	            // Get the uploaded file parameters
	            String fieldName = fi.getFieldName();
	            String fileName = fi.getName();
	            boolean isInMemory = fi.isInMemory();
	            long sizeInBytes = fi.getSize();
	            // Write the file
	            Random rnd = new Random();
	            if( fileName.lastIndexOf("\\") >= 0 ){
	            	file = new File( filePath + "SIP" + System.currentTimeMillis()
	            	+ new Integer(rnd.nextInt(90000) + 10000) + "_" + fileName.substring( fileName.lastIndexOf("\\"))) ;
	            	//out.println("first_case <br>");
	            }else{
	           		file = new File( filePath + "SIP" + System.currentTimeMillis()
	    	        + new Integer(rnd.nextInt(90000) + 10000) + "_" +	fileName.substring(fileName.lastIndexOf("\\")+1)) ;
	           		//out.println("second_case <br>");
	            }
	            fi.write( file ) ;
	            
	            out.println("Uploaded Filename: " + file.getAbsolutePath() + "<br>");
	            out.println("Indexing in Solr complete...... <span class=\"glyphicon glyphicon-ok\" style=\"color:green\" aria-hidden=\"true\"></span>");
	            }
	         }
	         Ingest.ingest_sip(file,community_path,collection_name);
	      }catch(Exception ex) {
	         System.out.println(ex);
	      }
	   }else{
	     
	      out.println("<p>No file uploaded</p>"); 
	      
	   }
	
	%>
