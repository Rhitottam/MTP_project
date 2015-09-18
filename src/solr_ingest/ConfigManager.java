package solr_ingest;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigManager {
	private static Properties props = new Properties();
	private static InputStream input = null;
	public static Properties getProps(){
		try {
			System.out.println("Reading config.properties file......");
			System.out.println(System.getProperty("user.dir"));
			input = new FileInputStream("solr_upload_config.properties");
			System.out.println("Loading config.properties file......");
			props.load(input);
			System.out.println("OUTPUT FOLDER:"+props.get("output"));
			System.out.println("INPUT FOLDER:"+props.get("input"));
			System.out.println("CONTENT FOLDER:"+props.get("contents"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return props;
	}
}
