package org.apache.olingo.odata2.sample.service;

import static org.apache.olingo.odata2.sample.service.MyEdmProvider.ENTITY_SET_NAME_CARS;
import static org.apache.olingo.odata2.sample.service.MyEdmProvider.ENTITY_SET_NAME_STATES;
import static org.apache.olingo.odata2.sample.service.MyEdmProvider.ENTITY_SET_NAME_COUNTRIES;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmLiteralKind;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmSimpleType;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties.ODataEntityProviderPropertiesBuilder;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.exception.ODataNotFoundException;
import org.apache.olingo.odata2.api.exception.ODataNotImplementedException;
import org.apache.olingo.odata2.api.processor.ODataResponse;
import org.apache.olingo.odata2.api.processor.ODataSingleProcessor;
import org.apache.olingo.odata2.api.uri.KeyPredicate;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntityUriInfo;

public class MyODataSingleProcessor extends ODataSingleProcessor {
  
  private final DataStore dataStore;
  
  public MyODataSingleProcessor() {
    dataStore = new DataStore();
  }

  @Override
  public ODataResponse readEntitySet(GetEntitySetUriInfo uriInfo, String contentType) throws ODataException {
	try {
	    EdmEntitySet entitySet;
	
	    if (uriInfo.getNavigationSegments().size() == 0) {
	      entitySet = uriInfo.getStartEntitySet();

	      if (ENTITY_SET_NAME_STATES.equals(entitySet.getName())) {
	        return EntityProvider.writeFeed(contentType, entitySet, dataStore.all(), EntityProviderWriteProperties.serviceRoot(getContext().getPathInfo().getServiceRoot()).build());
	      } else if (ENTITY_SET_NAME_COUNTRIES.equals(entitySet.getName())) {
	        return EntityProvider.writeFeed(contentType, entitySet, dataStore.getManufacturers(), EntityProviderWriteProperties.serviceRoot(getContext().getPathInfo().getServiceRoot()).build());
	      } else {
              return EntityProvider.writeFeed(contentType,
                      entitySet,
                      DB.all(entitySet.getName()),
                      EntityProviderWriteProperties.serviceRoot(getContext().getPathInfo().getServiceRoot()).build());
          }
	
	      //throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
	
	    } else if (uriInfo.getNavigationSegments().size() == 1) {
	      //navigation first level, simplified example for illustration purposes only
	      entitySet = uriInfo.getTargetEntitySet();
	
	      if (ENTITY_SET_NAME_STATES.equals(entitySet.getName())) {
	    	  String manufacturerKey = getKeyValue(uriInfo.getKeyPredicates().get(0));
	
	        List<Map<String, Object>> cars = new ArrayList<Map<String, Object>>();
	        cars.addAll(dataStore.getCarsFor(manufacturerKey));
	
	        return EntityProvider.writeFeed(contentType, entitySet, cars, EntityProviderWriteProperties.serviceRoot(getContext().getPathInfo().getServiceRoot()).build());
	      } else {
              // TODO: fix possible SQL injection bug

              // TODO: figure out how to make other types of filters

              // TODO: figure out how to return counts
              return EntityProvider.writeFeed(contentType,
                      entitySet,
                      DB.all(entitySet.getName(),
                             uriInfo.getKeyPredicates().get(0)),
                      EntityProviderWriteProperties.serviceRoot(getContext().getPathInfo().getServiceRoot()).build());
          }
	    }
	
	    throw new ODataNotImplementedException();
	}
    catch (Exception e) {
    	e.printStackTrace();
    }
    return null;
  }

  @Override
  public ODataResponse readEntity(GetEntityUriInfo uriInfo, String contentType) throws ODataException {

	try {
	    if (uriInfo.getNavigationSegments().size() == 0) {
	      EdmEntitySet entitySet = uriInfo.getStartEntitySet();
	
	      if (ENTITY_SET_NAME_STATES.equals(entitySet.getName())) {
	    	  String id = getKeyValue(uriInfo.getKeyPredicates().get(0));
	        Map<String, Object> data;
	
			data = dataStore.id(id);
			
	        if (data != null) {
	          URI serviceRoot = getContext().getPathInfo().getServiceRoot();
	          ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(serviceRoot);
	
	          return EntityProvider.writeEntry(contentType, entitySet, data, propertiesBuilder.build());
	        }
	      } else if (ENTITY_SET_NAME_COUNTRIES.equals(entitySet.getName())) {
	    	  String id = getKeyValue(uriInfo.getKeyPredicates().get(0));
	        Map<String, Object> data = dataStore.getManufacturer(id);
	
	        if (data != null) {
	          URI serviceRoot = getContext().getPathInfo().getServiceRoot();
	          ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(serviceRoot);
	
	          return EntityProvider.writeEntry(contentType, entitySet, data, propertiesBuilder.build());
	        }
	      }
	
	      throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
	
	    } else if (uriInfo.getNavigationSegments().size() == 1) {
	      //navigation first level, simplified example for illustration purposes only
	      EdmEntitySet entitySet = uriInfo.getTargetEntitySet();
	      
	      Map<String, Object> data = null;
	      
	      if (ENTITY_SET_NAME_COUNTRIES.equals(entitySet.getName())) {
	        String carKey = getKeyValue(uriInfo.getKeyPredicates().get(0));
	        data = dataStore.getManufacturerFor("" + carKey);
	      }
	      
	      if(data != null) {
	        return EntityProvider.writeEntry(contentType, uriInfo.getTargetEntitySet(), 
	            data, EntityProviderWriteProperties.serviceRoot(getContext().getPathInfo().getServiceRoot()).build());
	      }
	
	      throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
	    }
	
	    throw new ODataNotImplementedException();
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return null;

  }
  
  private String getKeyValue(KeyPredicate key) throws ODataException {
    EdmProperty property = key.getProperty();
    EdmSimpleType type = (EdmSimpleType) property.getType();
    return type.valueOfString(key.getLiteral(), EdmLiteralKind.DEFAULT, property.getFacets(), String.class);
  }
}
