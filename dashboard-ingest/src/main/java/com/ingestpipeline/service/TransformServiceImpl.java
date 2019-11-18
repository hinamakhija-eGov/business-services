package com.ingestpipeline.service;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.ingestpipeline.util.Constants;
	
/**
 * This is a Service Implementation for all the actions which are with respect to Elastic Search 
 * @author Darshan Nagesh
 *
 */
@Service(Constants.Qualifiers.TRANSFORM_SERVICE)
public class TransformServiceImpl implements TransformService {
	
	public static final Logger LOGGER = LoggerFactory.getLogger(TransformServiceImpl.class);
	private static final String SEPARATOR = "_"; 
	private static final String JSON_EXTENSION = ".json"; 
	private static final String OBJECTIVE = "transform"; 
	private static final String CONFIGROOT = "config/";

	@Override
	public Boolean transformData(Map incomingData) {
		String dataContext = incomingData.get(Constants.DATA_CONTEXT).toString(); 
		String dataContextVersion = incomingData.get(Constants.DATA_CONTEXT_VERSION).toString(); 
		String sourceUrl = CONFIGROOT.concat(OBJECTIVE.concat(SEPARATOR).concat(dataContext).concat(SEPARATOR).concat(dataContextVersion).concat(JSON_EXTENSION));
		List chainrSpecJSON = JsonUtils.jsonToList(this.getClass().getClassLoader().getResourceAsStream(sourceUrl));
        Chainr chainr = Chainr.fromSpec( chainrSpecJSON );
        Object inputJSON = incomingData.get(Constants.DATA_OBJECT);
        try { 
            Object transformedOutput = chainr.transform( inputJSON );
            incomingData.put(Constants.DATA_OBJECT , transformedOutput);
            return Boolean.TRUE; 
        } catch (Exception e) { 
        	LOGGER.error("Encountered an error while tranforming the JSON : " + e.getMessage());
        	return Boolean.FALSE; 
        }
	}
}
 