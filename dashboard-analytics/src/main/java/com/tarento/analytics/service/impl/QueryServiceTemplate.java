package com.tarento.analytics.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tomcat.util.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static javax.servlet.http.HttpServletRequest.BASIC_AUTH;
import static org.apache.commons.codec.CharEncoding.US_ASCII;
import static org.springframework.http.HttpHeaders.AUTHORIZATION;

@Component
public class QueryServiceTemplate {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryServiceTemplate.class);

    @Value("${egov.services.esindexer.host.name}")
    private String indexServiceHost;
    @Value("${egov.services.esindexer.host.search}")
    private String indexServiceHostSearch;
    @Value("${services.esindexer.host}")
    private String dssindexServiceHost;
    @Value("${egov-es-username}")
    private String userName;
    @Value("${egov-es-password}")
    private String password;


    @Autowired
    private RestTemplate restTemplate;


    public JsonNode search(String index, String searchQuery) throws IOException {

        //TODO remove the check for dssindexServiceHost
        String url =(/*index.contains("dss") ? dssindexServiceHost :*/ indexServiceHost) + index + indexServiceHostSearch;
        HttpHeaders headers = /*index.contains("dss") ? new HttpHeaders() :*/ getHttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        LOGGER.info("Index Name : " + index); 
        LOGGER.info("Searching ES for Query: " + searchQuery); 
        HttpEntity<String> requestEntity = new HttpEntity<>(searchQuery, headers);
        String reqBody = requestEntity.getBody();
        JsonNode responseNode = null;

        try {
            ResponseEntity<Object> response = restTemplate.exchange(url, HttpMethod.POST, requestEntity, Object.class);
            responseNode = new ObjectMapper().convertValue(response.getBody(), JsonNode.class);
            LOGGER.info("RestTemplate response :- "+responseNode);

        } catch (HttpClientErrorException e) {
            e.printStackTrace();
            LOGGER.error("client error while searching ES : " + e.getMessage());
        }
        return responseNode;
    }

    private HttpHeaders getHttpHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.add(AUTHORIZATION, getBase64Value(userName, password));
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        headers.setContentType(MediaType.APPLICATION_JSON);

        List<MediaType> mediaTypes = new ArrayList<>();
        mediaTypes.add(MediaType.APPLICATION_JSON);
        headers.setAccept(mediaTypes);
        return headers;
    }

    /**
     * Helper Method to create the Base64Value for headers
     *
     * @param userName
     * @param password
     * @return
     */
    private String getBase64Value(String userName, String password) {
        String authString = String.format("%s:%s", userName, password);
        byte[] encodedAuthString = Base64.encodeBase64(authString.getBytes(Charset.forName(US_ASCII)));
        return String.format(BASIC_AUTH, new String(encodedAuthString));
    }

}
