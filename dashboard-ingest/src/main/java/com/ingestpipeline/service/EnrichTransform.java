package com.ingestpipeline.service;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ingestpipeline.util.ConfigLoader;

@Service
public class EnrichTransform {
	
	public static final Logger LOGGER = LoggerFactory.getLogger(EnrichTransform.class);

    @Autowired
    private TransformService transformService;
    
    @Autowired
    private ConfigLoader configLoader; 

    private static final String SEPARATOR = "_";
    private static final String JSON_EXTENSION = ".json";
    private static final String OBJECTIVE = "transform";
    private static final String CONFIGROOT = "config/";
    private static final String VERSION = "v1";



    /**
     * Tranforms domain raw response from elastic search
     * This transformation is specific to domain objects
     *
     * @param rawResponseNode
     * @param businessService
     * @return
     */
    public Object transform (Map rawResponseNode, String businessService) {
    	
    	ObjectMapper mapper = new ObjectMapper(); 
		List chainrSpecJSON = null ;
		try {
			chainrSpecJSON = mapper.readValue(configLoader.get(OBJECTIVE.concat(SEPARATOR).concat(businessService.toLowerCase()).concat(SEPARATOR).concat(VERSION).concat(JSON_EXTENSION)), List.class);
		} catch (Exception e) {
			LOGGER.error("Encountered an error : " + e.getMessage());
		}
        Chainr chainr = Chainr.fromSpec( chainrSpecJSON );

        Object indexData = rawResponseNode.keySet().contains("_source") ? ((Map)rawResponseNode.get("_source")).get("Data") : null;
        Object transNode = indexData!= null ? chainr.transform(indexData) : null;

        return transNode;

    }
}
