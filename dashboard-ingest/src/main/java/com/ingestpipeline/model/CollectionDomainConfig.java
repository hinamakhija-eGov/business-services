package com.ingestpipeline.model;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.ingestpipeline.util.ConfigLoader;

//@JsonIgnoreProperties(ignoreUnknown=true)
@Component("CollectionDomainConfig")
public class CollectionDomainConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionDomainConfig.class);
    private static final String COLLECTION_DOMAIN_CONFIG = "CollectionDomainConfig.json";


    @Autowired
    private ConfigLoader configLoader;
    /**
     * Holds domain name as key and it's index config detail as value.
     */
    private Map<String, DomainIndexConfig>  domainIndexConfigMap = new HashMap<>();

    public void putDomain(String domainName, DomainIndexConfig domainIndexConfig){
        domainIndexConfigMap.put(domainName, domainIndexConfig);
    }

    public DomainIndexConfig getIndexConfig(String domainName){
        return domainIndexConfigMap.get(domainName);
    }


    /**
     * loads once on application start up.
     */
    public void loadCollectionDomains(){
        //TODO:- load the JSON mapping, read/prepare the n queries
        String collectionConfigContent = configLoader.get(COLLECTION_DOMAIN_CONFIG);
        LOGGER.info("collectionConfigContent json string = "+collectionConfigContent);

        try{

            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = new ObjectMapper().readTree(collectionConfigContent);
            ArrayNode domainConfigArr = (ArrayNode) root.path("domainConfig");

            Iterator<JsonNode> iterator = domainConfigArr.elements();
            while (iterator.hasNext()) {
                DomainIndexConfig domainIndexConfig = mapper.readValue(iterator.next().toString(), DomainIndexConfig.class);
                System.out.println("DomainIndexConfig id=" + domainIndexConfig.getBusinessType());
                domainIndexConfigMap.put(domainIndexConfig.getBusinessType(), domainIndexConfig);

            }
            LOGGER.info("After loading, domainIndexConfigMap size  = "+ domainIndexConfigMap.size());

        } catch (Exception e){
            e.printStackTrace();
            LOGGER.error("on construction domain collection map: "+ e.getMessage());
        }

    }



/*    private List<DomainIndexConfig> domainIndexConfig;
    @JsonProperty(value="domainConfig")
    public List<DomainIndexConfig> getDomainIndexConfig() {
        return domainIndexConfig;
    }

    public void setDomainIndexConfig(List<DomainIndexConfig> domainIndexConfig) {
        this.domainIndexConfig = domainIndexConfig;
    }*/

}
