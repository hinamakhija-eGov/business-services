package com.tarento.analytics.org.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.tarento.analytics.constant.Constants;
import com.tarento.analytics.service.impl.RestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Component
public class MdmsService {

    private static Logger logger = LoggerFactory.getLogger(MdmsService.class);

    private Map<String, String> ddrTenantMapping = new HashMap<>();

    @Value("${egov.mdms-service.target.url}")
    private String mdmsServiceSearchUri;

    @Autowired
    private RestService restService;

    @Autowired
    private ObjectMapper mapper;

    @Value("${egov.mdms-service.request}")
    private  String REQUEST_INFO_STR ;//="{\"RequestInfo\":{\"authToken\":\"\"},\"MdmsCriteria\":{\"tenantId\":\"pb\",\"moduleDetails\":[{\"moduleName\":\"tenant\",\"masterDetails\":[{\"name\":\"tenants\"}]}]}}";


    @PostConstruct
    public void loadMdmsService() throws Exception{

        JsonNode requestInfo = mapper.readTree(REQUEST_INFO_STR);
        JsonNode response = restService.post(mdmsServiceSearchUri, "", requestInfo);
        ArrayNode tenants = (ArrayNode) response.findValues(Constants.MDMSKeys.TENANTS).get(0);


        for(JsonNode tenant : tenants) {
            JsonNode ddrCode = tenant.findValue(Constants.MDMSKeys.DISTRICT_CODE);
            JsonNode ddrName = tenant.findValue(Constants.MDMSKeys.DDR_NAME);

            if (!ddrTenantMapping.containsKey(ddrCode.asText())){
                ddrTenantMapping.put(ddrCode.asText(), ddrName.asText());
            }
        }

        logger.info("ddrTenantMapping = "+ddrTenantMapping);
    }

    public String getDDRNameByCode(String ddrCode){
        return ddrTenantMapping.getOrDefault(ddrCode, "");
    }


}
