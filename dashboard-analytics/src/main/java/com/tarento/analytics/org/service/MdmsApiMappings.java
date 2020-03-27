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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class MdmsApiMappings {

    private boolean isTranslate = Boolean.FALSE;
    private final String TESTING_ID = "pb.testing";
    private final String NAME = "name";

    private static Logger logger = LoggerFactory.getLogger(MdmsApiMappings.class);

    private Map<String, String> ddrTenantMapping = new HashMap<>();
    private Map<String, List<String>> ddrTenantMapping1 = new HashMap<>();
    private Map<String, String> codeValues = new HashMap<>();
    private Map<String, List<String>> ddrValueMap = new HashMap<>();

    @Value("${egov.mdms-service.target.url}")
    private String mdmsServiceSearchUri;

    @Autowired
    private RestService restService;

    @Autowired
    private ObjectMapper mapper;

    @Value("${egov.mdms-service.request}")
    private  String REQUEST_INFO_STR ;//="{\"RequestInfo\":{\"authToken\":\"\"},\"MdmsCriteria\":{\"tenantId\":\"pb\",\"moduleDetails\":[{\"moduleName\":\"tenant\",\"masterDetails\":[{\"name\":\"tenants\"}]}]}}";

    public String valueOf(String code){
        return codeValues.getOrDefault(code, null);
    }

    public void setTranslate(boolean isTranslate){
        this.isTranslate = isTranslate;
    }

    @PostConstruct
    public void loadMdmsService() throws Exception {

        JsonNode requestInfo = mapper.readTree(REQUEST_INFO_STR);
        try {
            JsonNode response = restService.post(mdmsServiceSearchUri, "", requestInfo);
            ArrayNode tenants = (ArrayNode) response.findValues(Constants.MDMSKeys.TENANTS).get(0);


            for(JsonNode tenant : tenants) {
                JsonNode tenantId = tenant.findValue(Constants.MDMSKeys.CODE);
                JsonNode ddrCode = tenant.findValue(Constants.MDMSKeys.DISTRICT_CODE);
                JsonNode ddrName = tenant.findValue(Constants.MDMSKeys.DDR_NAME);

                JsonNode name = tenant.findValue(NAME);
                if(!codeValues.containsKey(tenantId.asText())) codeValues.put(tenantId.asText(), name.asText());


                if(!tenantId.asText().equalsIgnoreCase(TESTING_ID)) {
                    if(!ddrTenantMapping1.containsKey(ddrName.asText())){
                        List<String> tenantList = new ArrayList<>();
                        tenantList.add(tenantId.asText());
                        ddrTenantMapping1.put(ddrName.asText(),tenantList);
                        List<String> values = new ArrayList<>();
                        values.add(name.asText());
                        ddrValueMap.put(ddrName.asText(), values);

                    } else {
                        ddrTenantMapping1.get(ddrName.asText()).add(tenantId.asText());
                        ddrValueMap.get(ddrName.asText()).add(name.asText());

                    }

                    if (!ddrTenantMapping.containsKey(ddrCode.asText())){
                        ddrTenantMapping.put(ddrCode.asText(), ddrName.asText());
                    }
                }

            }
        } catch (Exception e){
            getDefaultMapping();
            logger.error("Loading Mdms service error: "+e.getMessage()+" :: loaded default DDRs");
        }
        //logger.info("ddrTenantMapping = "+ddrTenantMapping);
        logger.info("ddrTenantMapping1 = "+ddrTenantMapping1);

    }

    public String getDDRNameByCode(String ddrCode){
        return ddrTenantMapping.getOrDefault(ddrCode, "");
    }

    public List<String> getTenantIds(String ddrCode){
        return ddrTenantMapping1.getOrDefault(ddrCode, new ArrayList<>());
    }

    public String getDDRName(String tenantId){

        for(Map.Entry entry : isTranslate ? ddrValueMap.entrySet() :ddrTenantMapping1.entrySet()){
            List<String> values = (List<String>) entry.getValue();
            if(values.contains(tenantId)) return entry.getKey().toString();

        }
        return null;

    }

    public Map<String, List<String>> getGroupedTenants(List<String> tenants){

        Map<String, List<String>> groupTenantIds = new HashMap<>();

        if(tenants!=null){
            for(String tenant : tenants) {

                String ddrName = getDDRName(tenant);
                if (groupTenantIds.containsKey(ddrName)){
                    groupTenantIds.get(ddrName).add(tenant);

                } else {
                    List<String> tenantList = new ArrayList<>();
                    tenantList.add(tenant);
                    groupTenantIds.put(ddrName,tenantList);
                }

            }
        }

        return groupTenantIds;
    }


    public Map<String, List<String>> getAll(){
        return isTranslate ? ddrValueMap : ddrTenantMapping1;
    }

    private void getDefaultMapping(){

        ddrTenantMapping.put("1", "Amritsar-DDR");
        ddrTenantMapping.put("2", "Patiala-DDR");
        ddrTenantMapping.put("3", "Bathinda-DDR");
        ddrTenantMapping.put("4", "Ferozepur-DDR");
        ddrTenantMapping.put("5", "Ludhiana-DDR");
        ddrTenantMapping.put("6", "Ferozepur-DDR");
        ddrTenantMapping.put("7", "Ferozepur-DDR");
        ddrTenantMapping.put("8", "Amritsar-DDR");
        ddrTenantMapping.put("9", "Jalandhar-DDR");
        ddrTenantMapping.put("10", "Jalandhar-DDR");

        ddrTenantMapping.put("11", "Jalandhar-DDR");
        ddrTenantMapping.put("12", "Ludhiana-DDR");
        ddrTenantMapping.put("13", "Bathinda-DDR");
        ddrTenantMapping.put("14", "Ferozepur-DDR");
        ddrTenantMapping.put("15", "Patiala-DDR");
        ddrTenantMapping.put("16", "Bathinda-DDR");
        ddrTenantMapping.put("17", "Jalandhar-DDR");
        ddrTenantMapping.put("18", "Pathankot-MC");
        ddrTenantMapping.put("19", "Patiala-DDR");
        ddrTenantMapping.put("20", "Ludhiana-DDR");
        ddrTenantMapping.put("21", "Patiala-DDR");
        ddrTenantMapping.put("22", "Bathinda-DDR");
        ddrTenantMapping.put("140001", "Ludhiana-DDR");

    }


}

