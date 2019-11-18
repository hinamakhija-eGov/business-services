package com.ingestpipeline.service;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class EnrichTransform {

    @Autowired
    private TransformService transformService;

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

        String sourceUrl = CONFIGROOT.concat(OBJECTIVE.concat(SEPARATOR).concat(businessService.toLowerCase()).concat(SEPARATOR).concat(VERSION).concat(JSON_EXTENSION));
        List chainrSpecJSON = JsonUtils.jsonToList(this.getClass().getClassLoader().getResourceAsStream(sourceUrl));
        Chainr chainr = Chainr.fromSpec( chainrSpecJSON );

        Object indexData = rawResponseNode.keySet().contains("_source") ? ((Map)rawResponseNode.get("_source")).get("Data") : null;
        Object transNode = indexData!= null ? chainr.transform(indexData) : null;

        return transNode;

    }
}
