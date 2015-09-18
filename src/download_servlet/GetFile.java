package download_servlet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.common.SolrException;
import org.apache.tika.Tika;

import solr_ingest.PostgresDatabaseManager;

public class GetFile extends HttpServlet{
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException{
		System.out.println("here................");
		
		System.out.println(request.getPathInfo());
		if(request.getPathInfo()==null){
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "id missing");
		    return;
		}
		//String param = (String)request.getParameter("id");
	    /*if (param == null)
	    {
	        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Parameter id missing");
	        return;
	    }*/
		String id = request.getPathInfo().substring(request.getPathInfo().lastIndexOf("/")+1);
		//String id = request.getParameter("id");
		PostgresDatabaseManager.connect();
		String path = PostgresDatabaseManager.getPath(id);
		if(path.equalsIgnoreCase("none")){
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "no content");
		    return;
		}
		else{
			File dwnld_file = new File(path);
			Tika tika =  new Tika();
			response.setContentType(tika.detect(dwnld_file));   
			response.setHeader("Content-Disposition","attachment; filename=\"" + path.substring(path.lastIndexOf(File.separator)) + "\"");   
			FileInputStream fileInputStream=new FileInputStream(path);  
			OutputStream out = response.getOutputStream();
			int i=0;
			//byte[] bytes = new byte[BYTE];
			
			while ((i=fileInputStream.read()) != -1) {  
				out.write(i);   
			} 
			out.flush();
			out.close();
			fileInputStream.close();
		}
	}
}
