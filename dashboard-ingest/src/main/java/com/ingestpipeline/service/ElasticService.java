package com.ingestpipeline.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.eclipsesource.json.JsonObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.ingestpipeline.model.IncomingData;
import com.ingestpipeline.model.TargetData;
import com.ingestpipeline.util.Constants;

@Component("elasticService")
public class ElasticService implements IESService {

	@Value("${es.index.type}")
	public String DOC_PATH;

	@Value("${services.esindexer.host}")
	private String indexServiceHost;
	@Value("${egov.services.esindexer.host.search}")
	private String indexServiceHostSearch;

	@Value("${es.host.schema}")
	private String schema;

	@Value("${services.esindexer.host}")
	private String indexerServiceHost;
	@Value("${es.target.index.name}")
	private String targetIndexName;

	@Value("${es.index.name}")
	private String collectionIndexName;
	
	@Value("${es.index.searchQuery.collection}")
	private String searchQueryCollection;
	
	@Value("${es.index.searchQuery.billing}")
	private String searchQueryBilling;

	@Autowired
	private RestTemplate restTemplate;
	
	@Autowired
	private IngestService ingestService;

	private static final String SLASH_SEPERATOR  = "/";

	public static final Logger LOGGER = LoggerFactory.getLogger(ElasticService.class);

	public String getSearchQueryCollection() {
		return searchQueryCollection;
	}

	public void setSearchQueryCollection(String searchQueryCollection) {
		this.searchQueryCollection = searchQueryCollection;
	}

	public String getSearchQueryBilling() {
		return searchQueryBilling;
	}

	public void setSearchQueryBilling(String searchQueryBilling) {
		this.searchQueryBilling = searchQueryBilling;
	}


	public JsonNode post(String index, String type, String id, String authToken, String requestNode) {

		StringBuilder uriBuilder = new StringBuilder(indexerServiceHost.concat(index).concat(SLASH_SEPERATOR).concat(type).concat(SLASH_SEPERATOR).concat(id));
		//String uri = indexServiceHost + index + SLASH_SEPERATOR + type + SLASH_SEPERATOR + id;
		HttpHeaders headers = new HttpHeaders();
		if(authToken != null && !authToken.isEmpty())
			headers.add("Authorization", "Bearer "+ authToken );
		headers.setContentType(MediaType.APPLICATION_JSON);

		LOGGER.info("Request Node: " + requestNode);
		HttpEntity<String> requestEntity = null;
		if(requestNode != null ) requestEntity = new HttpEntity<>(requestNode, headers);
		else requestEntity = new HttpEntity<>("{}", headers);

		JsonNode responseNode = null;

		try {
			ResponseEntity<Object> response = restTemplate.postForEntity(uriBuilder.toString(), requestEntity, Object.class);
			//restTemplate.postForEntity(uri,requestEntity);
			responseNode = new ObjectMapper().convertValue(response.getBody(), JsonNode.class);
			LOGGER.info("RestTemplate response :- "+responseNode);

		} catch (HttpClientErrorException e) {
			LOGGER.error("post client exception: " + e.getMessage());
		}
		return responseNode;
	}

	@Override
	public JsonNode search(String index, ObjectNode searchQuery) throws IOException {
		SearchRequest searchRequest = buildSearchRequest(index, searchQuery);
		SearchResponse searchResponse = getClient(index, indexServiceHost, 9200, schema).search(searchRequest,
				RequestOptions.DEFAULT);

		ArrayNode resultArray = JsonNodeFactory.instance.arrayNode();
		for (SearchHit hit : searchResponse.getHits()) {
			JsonNode node = new ObjectMapper().readValue(hit.getSourceAsString(), JsonNode.class);
			resultArray.add(node);
		}
		return resultArray;
	}
	
	@Override
    public Map search(String index, String searchQuery) throws Exception {

        String url = indexServiceHost + index + indexServiceHostSearch;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        LOGGER.info("searching ES for query::" + searchQuery + "::on::" + index + "::ON URL::" + url);

        HttpEntity<String> requestEntity = new HttpEntity<>(searchQuery, headers);

        try {
			ResponseEntity<Object> response = restTemplate.exchange(url, HttpMethod.POST, requestEntity, Object.class);
			// LOGGER.info("Response Body::" + response.getBody());
            Map responseNode = new ObjectMapper().convertValue(response.getBody(), Map.class);
			// LOGGER.info("REsponse Node::" + responseNode);
			Map hits = (Map)responseNode.get("hits");
			// LOGGER.info("hits::" + hits);
            if((Integer)hits.get("total") >=1)
                return (Map)((ArrayList)hits.get("hits")).get(0);

        } catch (HttpClientErrorException e) {
            e.printStackTrace();
            LOGGER.error("client error while searching ES : " + e.getMessage());

        }
        return null;
    }
	
	@Override
	public Boolean push(Map requestBody) throws Exception {

		Object id = requestBody.get(Constants.IDENTIFIER);
		Object trxid = ((Map)requestBody.get(Constants.DATA_OBJECT)).get(Constants.TRANSACTION_ID);
		LOGGER.info("request body on ### trxid "  +trxid);


		String docId = id!=null ? id.toString(): trxid.toString();


		String url = indexerServiceHost + collectionIndexName +"/"+ DOC_PATH +"/"+ docId;
		LOGGER.info("url ## " +url);


		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);

		LOGGER.info("Posting request to ES on " + collectionIndexName + "with doc id: "+docId);

		JsonNode request = new ObjectMapper().convertValue(requestBody, JsonNode.class);
		LOGGER.info(" new request body json ### " +request);


		HttpEntity<String> requestEntity = new HttpEntity<>(request.toString(), headers);
		ArrayNode hitNodes = null;

		try {
			ResponseEntity<Object> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, Object.class);
			LOGGER.info("Status code on pushing to collection index : " + response.getStatusCode());
			if (response.getStatusCode().value() == HttpStatus.CREATED.value())
				return Boolean.TRUE;

		} catch (HttpClientErrorException e) {
			e.printStackTrace();
			LOGGER.error("client error while pushing ES collection index : " + e.getMessage());

		}
		return Boolean.FALSE;
	}

	@Override
	public Boolean push(TargetData requestBody) throws Exception {

		Long currentDateTime = new Date().getTime();
		String url = indexerServiceHost + targetIndexName + DOC_TYPE + requestBody.getId();

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);

		LOGGER.info("Posting request to ES on ## " + targetIndexName);
		LOGGER.info("request body on ### " +requestBody);

		JsonNode request = new ObjectMapper().convertValue(requestBody, JsonNode.class);

		HttpEntity<String> requestEntity = new HttpEntity<>(request.toString(), headers);
		ArrayNode hitNodes = null;

		try {
			ResponseEntity<Object> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, Object.class);
			LOGGER.info("Status code on pushing to target index : " + response.getStatusCode());
			if (response.getStatusCode().value() == 201)
				return Boolean.TRUE;

		} catch (HttpClientErrorException e) {
			e.printStackTrace();
			LOGGER.error("client error while pushing ES target index : " + e.getMessage());

		}
		return Boolean.FALSE;
	}
	
	@Override
	public Boolean searchIndex(String index, String searchQuery) throws Exception {
		LOGGER.info("searching ES for query: " + searchQuery + " on " + index);
		
		Map<String, String> scrollSearchParams = getScrollIdForScrollSearch(index); 
		
		new Thread(new Runnable() {
	        public void run(){
	        	int counter = 100; 
	    		int iSize = 0; 
	    		int iSumSize = 0; 
	    		int totalLimitSize = 100000; 
	        	Map<String, List<Object>> documentMap = new HashMap<>();
	        	while (iSize >= 0 && iSize < counter && iSumSize < totalLimitSize) {
	        		LOGGER.info("Sum Size is : " + iSumSize);
	    			documentMap = performScrollSearch(scrollSearchParams);
	    			List<Object> listOfDocs = documentMap.get("hits");
	    			iSize = listOfDocs.size();
	    			iSumSize = iSize + iSumSize;
	    			for (Map.Entry<String, List<Object>> entry : documentMap.entrySet()) {
	    				for (int i = 0; i < entry.getValue().size(); i++) {
	    					Map innerMap = (Map) entry.getValue().get(i);
	    					Gson gson = new Gson(); 
	    					String json = gson.toJson(innerMap.get("_source"));
	    				    ObjectMapper mapper = new ObjectMapper();
	    					JsonNode dataNode = null;
							try {
								dataNode = mapper.readTree(json);
							} catch (IOException e) {
								LOGGER.error("Encountered an exception while reading the JSON Node on Thread : " + e.getMessage());
							} 
	    					JsonNode dataObjectNode = dataNode.get("Data");
	    					Map<Object, Object> dataMap = new Gson().fromJson(
	    							dataObjectNode.toString(), new TypeToken<HashMap<Object, Object>>() {}.getType()
	    						);
							ingestService.ingestToPipeline(
									setIncomingData(scrollSearchParams.get("CONTEXT"), "v1", dataObjectNode));
	    				}
	    			}
	    		}
	        }
	    }).start();
		
		return Boolean.TRUE;
	}
	
	private IncomingData setIncomingData(String index, String version, Object documentValue) {
		IncomingData incomingData = new IncomingData();
		incomingData.setDataContext(index);
		incomingData.setDataContextVersion(version);
		incomingData.setDataObject(documentValue);
		return incomingData;
	}
	
	private Map performScrollSearch(Map<String, String> scrollSearchParams) { 
		Map<String, List<JsonObject>> hitsToMap = new LinkedHashMap();
		try {
			Map hits = new LinkedHashMap();
			HttpEntity<String> requestEntity = new HttpEntity<>(scrollSearchParams.get(Constants.ScrollSearch.QUERY), getHttpHeaders());
			ResponseEntity<Object> response = restTemplate.exchange(scrollSearchParams.get(Constants.ScrollSearch.SEARCH_PATH), HttpMethod.POST, requestEntity,
					Object.class);
			Map responseNode = new ObjectMapper().convertValue(response.getBody(), Map.class);
			hits = (Map) responseNode.get("hits");
			if ((Integer) hits.get("total") >= 1) {
				hitsToMap.put("hits", ((ArrayList) hits.get("hits")));
				return hitsToMap;
			}
		} catch (HttpClientErrorException e) {
			LOGGER.error("client error while searching ES : " + e.getMessage());
		}
		return hitsToMap;
	}
	
	private Map<String, String> getScrollIdForScrollSearch(String index) { 
		Map<String, String> scrollSearchParams = new HashMap<>(); 
		String queryString = null; 
		if (index.equals(Constants.ES_INDEX_COLLECTION)) {
			index = Constants.ES_INDEX_COLLECTION;
			queryString = getSearchQueryCollection();
			scrollSearchParams.put("CONTEXT", "collection"); 
		} else if (index.equals(Constants.ES_INDEX_BILLING)) {
			index = Constants.ES_INDEX_BILLING;
			queryString = getSearchQueryBilling();
			scrollSearchParams.put("CONTEXT", "billing"); 
		} else {
			index = "notDefinedIndex";
			queryString = "noquery";
		}
		String scrollUrl = indexServiceHost + index + indexServiceHostSearch + "?scroll=1m";
		HttpEntity<String> requestEntity = new HttpEntity<>(queryString, getHttpHeaders());
		String str = null;
		try {
			ResponseEntity<Object> response = restTemplate.exchange(scrollUrl, HttpMethod.POST, requestEntity,
					Object.class);
			Map responseNode = new ObjectMapper().convertValue(response.getBody(), Map.class);
			str = indexServiceHostSearch.replaceAll("[/]", "");
			scrollSearchParams.put(Constants.ScrollSearch.SCROLL_ID, (String) responseNode.get("_scroll_id"));
			scrollSearchParams.put(Constants.ScrollSearch.SEARCH_PATH, indexServiceHost + str + "/" + "scroll");
			String queryForScrollId = "{\"scroll\":\"1m\",\"scroll_id\":" + "\"" + scrollSearchParams.get(Constants.ScrollSearch.SCROLL_ID) + "\"" + "}";
			scrollSearchParams.put(Constants.ScrollSearch.QUERY, queryForScrollId); 
		} catch (HttpClientErrorException e) {
			LOGGER.error("client error while searching ES : " + e.getMessage());
		}
		return scrollSearchParams; 
	}
	
	private HttpHeaders getHttpHeaders() { 
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		return headers;
	}


}