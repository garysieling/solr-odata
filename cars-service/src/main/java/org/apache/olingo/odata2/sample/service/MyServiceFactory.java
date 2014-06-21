package org.apache.olingo.odata2.sample.service;

import org.apache.olingo.odata2.api.ODataCallback;
import org.apache.olingo.odata2.api.ODataDebugCallback;
import org.apache.olingo.odata2.api.ODataService;
import org.apache.olingo.odata2.api.ODataServiceFactory;
import org.apache.olingo.odata2.api.edm.provider.EdmProvider;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.processor.ODataContext;
import org.apache.olingo.odata2.api.processor.ODataSingleProcessor;

public class MyServiceFactory extends ODataServiceFactory {
	private final class ScenarioDebugCallback implements ODataDebugCallback {
		@Override
		public boolean isDebugEnabled()
		{ 
			return true; 
		}
	}
	
	private final class ScenarioErrorCallback implements ODataDebugCallback {
		@Override
		public boolean isDebugEnabled()
		{ 
			return true; 
		}
	}
  @Override
  public ODataService createService(ODataContext ctx) throws ODataException {

    EdmProvider edmProvider = new MyEdmProvider();

    ODataSingleProcessor singleProcessor = new MyODataSingleProcessor();

    return createODataSingleProcessorService(edmProvider, singleProcessor);
  }
  
  public <T extends ODataCallback> T getCallback(final Class<? extends ODataCallback> callbackInterface)
  { 
	  return (T) (callbackInterface.isAssignableFrom(ScenarioErrorCallback.class) ? 
			  new ScenarioErrorCallback() : 
				  callbackInterface.isAssignableFrom(ODataDebugCallback.class) ? 
						  new ScenarioDebugCallback() : 
							  super.getCallback(callbackInterface));
}

}
