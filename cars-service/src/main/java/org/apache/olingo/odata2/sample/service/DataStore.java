package org.apache.olingo.odata2.sample.service;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.olingo.odata2.api.edm.EdmSimpleTypeKind;
import org.codehaus.jackson.JsonNode;

public class DataStore {

  //Data accessors
  public Map<String, Object> id(String id) throws Exception {
	  JsonNode result = Solr.getJson(Solr.rootUrlId + id);
	  JsonNode solrJson = result.get("response").get("docs").get(0);
			  
	  return toDocument(solrJson);
  }
  
  public List<Map<String, Object>> all() throws Exception {
	  List<Map<String, Object>> documents = 
			  new ArrayList<Map<String, Object>>();
	  
	  JsonNode result = Solr.getJson(Solr.rootUrlData);
	  JsonNode solrJson = result.get("response").get("docs");
	  
	  for (int i = 0; i < solrJson.size(); i++) {
		  documents.add(toDocument(solrJson.get(i)));
	  }
			  
	  return documents;
  }
  
  public Map<String, Object> toDocument(JsonNode result) {
    Map<String, Object> data = null;

    Calendar updated = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    
    try {    	
    	Iterator<Entry<String, JsonNode>> fields = result.getFields();
    	
    	Map<String, Object> document =
    			new HashMap<String, Object>();
    	
    	Map<String, EdmSimpleTypeKind> types = 
    			Solr.getTypes();
    	
    	for (Entry<String, JsonNode> e = fields.next(); 
    		 fields.hasNext(); 
    		 e = fields.next()) 
    	{
    		String field = e.getKey();
    		JsonNode value = e.getValue();
    		
    		EdmSimpleTypeKind type = types.get(field);
    		if (EdmSimpleTypeKind.String.equals(type)) {
    			document.put(field, value.getTextValue());
			}
			else if (EdmSimpleTypeKind.Double.equals(type)) {
				document.put(field, value.asDouble());
			}
			else if (EdmSimpleTypeKind.Int64.equals(type)) {
				document.put(field, value.asInt());
			}
			else if (EdmSimpleTypeKind.Boolean.equals(type)) {
				document.put(field, value.asBoolean());
			}
    	}

    	document.put("Updated", updated);

    	return document;
    } catch (Exception e) {
    	System.out.println(e.getMessage());
    }
    
    
    /*switch (id) {
    case 1:
      updated.set(2012, 11, 11, 11, 11, 11);
      data = createCar(1, "F1 W03", 1, 189189.43, "EUR", "2012", updated, "file://imagePath/w03");
      break;

    case 2:
      updated.set(2013, 11, 11, 11, 11, 11);
      data = createCar(2, "F1 W04", 1, 199999.99, "EUR", "2013", updated, "file://imagePath/w04");
      break;

    case 3:
      updated.set(2012, 12, 12, 12, 12, 12);
      data = createCar(3, "F2012", 2, 137285.33, "EUR", "2012", updated, "http://pathToImage/f2012");
      break;

    case 4:
      updated.set(2013, 12, 12, 12, 12, 12);
      data = createCar(4, "F2013", 2, 145285.00, "EUR", "2013", updated, "http://pathToImage/f2013");
      break;

    case 5:
      updated.set(2011, 11, 11, 11, 11, 11);
      data = createCar(5, "F1 W02", 1, 167189.00, "EUR", "2011", updated, "file://imagePath/wXX");
      break;

    default:
      break;
    }*/
    

    return data;
  }

  
  public Map<String, Object> getManufacturer(String id) {
  Map<String, Object> data = null;
    Calendar date = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    
    if ("1".equals(id)) {
      Map<String, Object> addressStar = createAddress("Star Street 137", "Stuttgart", "70173", "Germany");
      date.set(1954, 7, 4);
      data = createManufacturer(1, "Star Powered Racing", addressStar, date);
    } else if ("2".equals(id)) {
      Map<String, Object> addressHorse = createAddress("Horse Street 1", "Maranello", "41053", "Italy");
      date.set(1929, 11, 16);
      data = createManufacturer(2, "Horse Powered Racing", addressHorse, date);
    }
    
    return data;
  }

/*
  private Map<String, Object> createCar(final int carId, final String model, final int manufacturerId,
      final double price,
      final String currency, final String modelYear, final Calendar updated, final String imagePath) {
    Map<String, Object> data = new HashMap<String, Object>();

    data.put("Id", carId);
    data.put("Model", model);
    data.put("ManufacturerId", manufacturerId);
    data.put("Price", price);
    data.put("Currency", currency);
    data.put("ModelYear", modelYear);
    data.put("Updated", updated);
    data.put("ImagePath", imagePath);

    return data;
  }*/

  
  private Map<String, Object> createManufacturer(int id, String name, Map<String, Object> address, Calendar updated) {
    Map<String, Object> data = new HashMap<String, Object>();
    data.put("Id", id);
    data.put("Name", name);
    data.put("Address", address);
    data.put("Updated", updated);
    return data;
  }
  
  private Map<String, Object> createAddress(String street, String city, String zipCode, String country) {
    Map<String, Object> address = new HashMap<String, Object>();
    address.put("Street", street);
    address.put("City", city);
    address.put("ZipCode", zipCode);
    address.put("Country", country);
    return address;
  }
  
  public List<Map<String, Object>> getManufacturers() {
    List<Map<String, Object>> manufacturers = new ArrayList<Map<String, Object>>();
    manufacturers.add(getManufacturer("0"));
    manufacturers.add(getManufacturer("1"));
    return manufacturers;
  }


  public List<Map<String, Object>> getCarsFor(String manufacturerId) throws Exception {
    List<Map<String, Object>> cars = all();
    List<Map<String, Object>> carsForManufacturer = new ArrayList<Map<String,Object>>();
    
    for (Map<String,Object> car: cars) {
      if(Integer.valueOf(manufacturerId).equals(car.get("ManufacturerId"))) {
        carsForManufacturer.add(car);
      }
    }
    
    return carsForManufacturer;
  }
  
  public Map<String, Object> getManufacturerFor(String carId) throws Exception {
    Map<String, Object> car = id(carId);
    if(car != null) {
      Object manufacturerId = car.get("ManufacturerId");
      if(manufacturerId != null) {
        return getManufacturer((String) manufacturerId);
      }
    }
    return null;
  }
}
