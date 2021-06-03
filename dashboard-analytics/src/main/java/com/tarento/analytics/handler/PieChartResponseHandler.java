package com.tarento.analytics.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.JsonPath;
import com.tarento.analytics.ConfigurationLoader;
import com.tarento.analytics.dto.AggregateDto;
import com.tarento.analytics.dto.AggregateRequestDto;
import com.tarento.analytics.dto.Data;
import com.tarento.analytics.dto.Plot;
import com.tarento.analytics.enums.ChartType;
import org.egov.common.contract.request.RequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tarento.analytics.constant.Constants.*;
import static java.util.Objects.isNull;

/**
 * This handles ES response for single index, multiple index to represent data as pie figure
 * Creates plots by merging/computing(by summation) index values for same key
 * AGGS_PATH : this defines the path/key to be used to search the tree
 * VALUE_TYPE : defines the data type for the value formed, this could be amount, percentage, number
 *
 */
@Component
public class PieChartResponseHandler implements IResponseHandler {
    public static final Logger logger = LoggerFactory.getLogger(PieChartResponseHandler.class);

    @Autowired
    private RestTemplate restTemplate;


    @Override
    public AggregateDto translate(AggregateRequestDto requestDto, ObjectNode aggregations) throws IOException {

        List<Data> dataList = new ArrayList<>();

        JsonNode aggregationNode = aggregations.get(AGGREGATIONS);
        JsonNode chartNode = requestDto.getChartNode();
        String headerKey = chartNode.get(CHART_NAME).asText();
        List<Plot> headerPlotList = new ArrayList<>();
        List<Double> totalValue = new ArrayList<>();

        String symbol = chartNode.get(IResponseHandler.VALUE_TYPE).asText();
        ArrayNode aggrsPaths = (ArrayNode) chartNode.get(IResponseHandler.AGGS_PATH);

        /**
         * For every aggregation on plot object is added
         */
        aggrsPaths.forEach(headerPath -> {
            aggregationNode.findValues(headerPath.asText()).stream().parallel().forEach(valueNode->{
                if(valueNode.has(BUCKETS)){
                    JsonNode buckets = valueNode.findValue(BUCKETS);
                    buckets.forEach(bucket -> {
                        Double val = valueNode.findValues(VALUE).isEmpty() ? bucket.findValue(DOC_COUNT).asInt() : bucket.findValue(VALUE).asDouble();
                        totalValue.add(val);
                        Plot plot = new Plot(bucket.findValue(KEY).asText(), val, symbol);
                        headerPlotList.add(plot);
                    });

                } else {
                    List<JsonNode> valueNodes = valueNode.findValues(VALUE).isEmpty() ? valueNode.findValues(DOC_COUNT) : valueNode.findValues(VALUE);
                    double sum = valueNodes.stream().mapToLong(o -> o.asLong()).sum();
                    totalValue.add(sum);
                    Plot plot = new Plot(headerPath.asText(), sum, symbol);
                    headerPlotList.add(plot);
                }
            });
        });

        Data data = new Data(headerKey, totalValue.stream().reduce(0.0, Double::sum), symbol);
        data.setPlots(headerPlotList);
        dataList.add(data);

        Map<String, String> localizationMessageCodeMap = new HashMap();
        fetchMapFromLocalization(localizationMessageCodeMap);
        dataList.forEach(data1 -> {
            if(!isNull(data1.getHeaderName()) && localizationMessageCodeMap.containsKey(data1.getHeaderName())){
                data1.setHeaderName(localizationMessageCodeMap.get(data1.getHeaderName()));
            }
            data1.getPlots().forEach(plot -> {
                if(!isNull(plot.getSymbol()) && plot.getSymbol().equals("text")){
                    if(!isNull(plot.getLabel()) && localizationMessageCodeMap.containsKey(plot.getLabel())) {
                        plot.setLabel(localizationMessageCodeMap.get(plot.getLabel()));
                    }
                }
            });
        });

        return getAggregatedDto(chartNode, dataList, requestDto.getVisualizationCode());

    }

    private void fetchMapFromLocalization(Map<String, String> localizationMessageCodeMap){
        RequestInfo requestInfo = new RequestInfo();
        StringBuilder localizationUri = new StringBuilder(LOCALIZATION_URL);
        Object result = null;
        List<String> codes = new ArrayList<>();
        List<String> messages = new ArrayList<>();

        Map<String, Object> request = new HashMap<>();
        request.put("RequestInfo", requestInfo);

        try {
            result = restTemplate.postForObject(localizationUri.toString(), request, Map.class);
            codes = JsonPath.read(result, LOCALIZATION_CODES_JSONPATH);
            messages = JsonPath.read(result, LOCALIZATION_MSGS_JSONPATH);
        } catch (Exception e) {
            logger.error("Exception while fetching from localization: " + e);
        }

        for(int i = 0; i < messages.size(); i++){
            localizationMessageCodeMap.put(messages.get(i), codes.get(i));
        }

    }
}
