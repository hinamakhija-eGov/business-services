package com.ingestpipeline.service;

import com.eclipsesource.json.JsonObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.ingestpipeline.model.TargetData;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import org.springframework.http.*;
import java.io.IOException;
import java.util.Date;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component("elasticService")
public class ElasticService implements IESService {

	@Value("${egov.services.esindexer.host.name}")
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

        LOGGER.info("searching ES for query: " + searchQuery + " on " + index);

        HttpEntity<String> requestEntity = new HttpEntity<>(searchQuery, headers);

        try {
            ResponseEntity<Object> response = restTemplate.exchange(url, HttpMethod.POST, requestEntity, Object.class);
            Map responseNode = new ObjectMapper().convertValue(response.getBody(), Map.class);
            Map hits = (Map)responseNode.get("hits");
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

		String trId = ((Map) requestBody.get("dataObject")).get("transactionId").toString();
		String url = indexerServiceHost + collectionIndexName + "/_doc/" + trId;

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);

		LOGGER.info("Posting request to ES on " + collectionIndexName);
		JsonNode request = new ObjectMapper().convertValue(requestBody, JsonNode.class);

		HttpEntity<String> requestEntity = new HttpEntity<>(request.toString(), headers);
		ArrayNode hitNodes = null;

		try {
			ResponseEntity<Object> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, Object.class);
			LOGGER.info("Status code on pushing to collection index : " + response.getStatusCode());
			if (response.getStatusCode().value() == 201)
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
		String url = indexerServiceHost + targetIndexName + "/_doc/" + requestBody.getId();

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);

		System.out.println("Posting request to ES on " + targetIndexName);
		JsonNode request = new ObjectMapper().convertValue(requestBody, JsonNode.class);

		HttpEntity<String> requestEntity = new HttpEntity<>(request.toString(), headers);
		ArrayNode hitNodes = null;

		try {
			ResponseEntity<Object> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, Object.class);
			System.out.println("Status code on pushing to target index : " + response.getStatusCode());
			if (response.getStatusCode().value() == 201)
				return Boolean.TRUE;

		} catch (HttpClientErrorException e) {
			e.printStackTrace();
			System.out.println("client error while pushing ES target index : " + e.getMessage());

		}
		return Boolean.FALSE;
	}
	
	@Override
	public Map searchIndex(String index, String searchQuery) throws Exception {
		String scrollUrl = indexServiceHost + index + indexServiceHostSearch + "?scroll=5m";
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);

		LOGGER.info("searching ES for query: " + searchQuery + " on " + index);

		HttpEntity<String> requestEntity = new HttpEntity<>(searchQuery, headers);
		Map hits = new LinkedHashMap();
		String scroll_id = null;
		String str = null;
		String queryForScrollId = null;
		String postByScrollId = null;
		Map<String, List<JsonObject>> hitsToMap = new LinkedHashMap();
		try {
			ResponseEntity<Object> response = restTemplate.exchange(scrollUrl, HttpMethod.POST, requestEntity,
					Object.class);
			Map responseNode = new ObjectMapper().convertValue(response.getBody(), Map.class);
			str = indexServiceHostSearch.replaceAll("[/]", "");
			scroll_id = (String) responseNode.get("_scroll_id");
			postByScrollId = indexServiceHost + str + "/" + "scroll";
			queryForScrollId = "{\"scroll\":\"5m\",\"scroll_id\":" + "\"" + scroll_id + "\"" + "}";

		} catch (HttpClientErrorException e) {
			e.printStackTrace();
			LOGGER.error("client error while searching ES : " + e.getMessage());
		}
		try {
			requestEntity = new HttpEntity<>(queryForScrollId, headers);
			ResponseEntity<Object> response = restTemplate.exchange(postByScrollId, HttpMethod.POST, requestEntity,
					Object.class);
			Map responseNode = new ObjectMapper().convertValue(response.getBody(), Map.class);
			hits = (Map) responseNode.get("hits");
			if ((Integer) hits.get("total") >= 1) {
				hitsToMap.put("hits", ((ArrayList) hits.get("hits")));
				return hitsToMap;
			}

		} catch (HttpClientErrorException e) {
			e.printStackTrace();
			LOGGER.error("client error while searching ES : " + e.getMessage());
		}
		return hits;
	}


}