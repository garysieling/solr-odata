package org.apache.olingo.odata2.sample.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.olingo.odata2.api.edm.EdmSimpleTypeKind;
import org.apache.olingo.odata2.api.edm.provider.Property;
import org.apache.olingo.odata2.api.edm.provider.SimpleProperty;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
public class Solr {
	public static String rootUrlTypes = "http://localhost:8983/solr/collection1/admin/luke?wt=json&show=schema&_=1402970090723";
	public static String rootUrlData = "http://localhost:8983/solr/collection1/odata?wt=json&indent=true&_=14028811429&q=*%3A*";
	public static String rootUrlId = "http://localhost:8983/solr/collection1/odata?wt=json&indent=true&_=14028811429&q=id%3A";
		
	private static final String USER_AGENT = "Mozilla/5.0";

	public static JsonNode getJson(String url) 
			throws Exception 
	{
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
 
		con.setRequestMethod("GET");
		con.setRequestProperty("User-Agent", USER_AGENT);
 
		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);
 
		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null)
		{
			response.append(inputLine);
		}
		
		in.close();
		
	    try {
			String json = response.toString();
			ObjectMapper mapper = new ObjectMapper();
			
			JsonNode rootNode = mapper.readValue(json, JsonNode.class);
			JsonParser jp = mapper.getJsonFactory().createJsonParser(json);		
			rootNode = mapper.readTree(jp);
			return rootNode;
		
	    }
	    catch (Exception e) {
	    	System.out.println(e.getMessage());
	    }
	    
		return null;
	}
	
	public static String getValue(
			JsonNode rootNode,
			String[] values)
	{
		for (String v : values) 
		{
			rootNode = rootNode.get(v);
		}
		
		return rootNode.asText();
	}

	public static Collection<? extends Property> getProperties() throws Exception {
		JsonNode root = getJson(rootUrlTypes);

		JsonNode fields = root.get("schema").get("fields");
		List<Property> props = new ArrayList<Property>(fields.size());
		
		Iterator<String> names = fields.getFieldNames();
		for (String name = names.next(); names.hasNext(); name = names.next()) {
			JsonNode f = fields.get(name);
						
			SimpleProperty p = new SimpleProperty().setName(name);
			
			String type = f.get("type").asText();
			
			if ("text_general".equals(type) || "string".equals(type)) {
				p.setType(EdmSimpleTypeKind.String);
				props.add(p);
			}
			else if ("float".equals(type)) {
				p.setType(EdmSimpleTypeKind.Double);
				props.add(p);
			}
			else if ("int".equals(type)) {
				p.setType(EdmSimpleTypeKind.Int64);
				props.add(p);
			}
			else if ("date".equals(type)) {
				p.setType(EdmSimpleTypeKind.String);

				//p.setType(EdmSimpleTypeKind.DateTime);
				props.add(p);
			}
			else if ("boolean".equals(type)) {
				p.setType(EdmSimpleTypeKind.Boolean);
				props.add(p);
			}
		
		}
		// TODO Auto-generated method stub
		return props;
	}	
	
	public static Map<String, EdmSimpleTypeKind> getTypes() throws Exception {
		JsonNode root = getJson(rootUrlTypes);

		JsonNode fields = root.get("schema").get("fields");
		Map<String, EdmSimpleTypeKind> props =
				new HashMap<String, EdmSimpleTypeKind>();
		
		Iterator<String> names = fields.getFieldNames();
		for (String name = names.next(); names.hasNext(); name = names.next()) {
			JsonNode f = fields.get(name);
						
			SimpleProperty p = new SimpleProperty().setName(name);
			
			String type = f.get("type").asText();
			
			if ("text_general".equals(type) || "string".equals(type)) {
				props.put(name, EdmSimpleTypeKind.String);
			}
			else if ("float".equals(type)) {
				props.put(name, EdmSimpleTypeKind.Double);
			}
			else if ("int".equals(type)) {
				props.put(name, EdmSimpleTypeKind.Int64);
			}
			else if ("date".equals(type)) {
				props.put(name, EdmSimpleTypeKind.String);
			}
			else if ("boolean".equals(type)) {	
				props.put(name, EdmSimpleTypeKind.Boolean);
			}
		
		}
		
		return props;
	}

}
