package com.tarento.analytics.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tarento.analytics.ConfigurationLoader;
import com.tarento.analytics.dto.AggregateDto;
import com.tarento.analytics.dto.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This handles ES response for single index, multiple index to represent single data value
 * Creates plots by merging/computing(by summation or by percentage) index values for same key
 * ACTION:  for the chart config defines the type either summation or computing percentage
 * AGGS_PATH : this defines the path/key to be used to search the tree
 *
 */
@Component
public class MetricChartResponseHandler implements IResponseHandler{
    public static final Logger logger = LoggerFactory.getLogger(MetricChartResponseHandler.class);

    @Autowired
    ConfigurationLoader configurationLoader;
    public final String PERCENTAGE = "percentage";


    @Override
    public AggregateDto translate(String chartId, ObjectNode aggregations) throws IOException {
        List<Data> dataList = new ArrayList<>();

        JsonNode aggregationNode = aggregations.get(AGGREGATIONS);
        JsonNode chartNode = configurationLoader.get(API_CONFIG_JSON).get(chartId);

        List<Double> totalValues = new ArrayList<>();
        String chartName = chartNode.get(CHART_NAME).asText();
        String action = chartNode.get(ACTION).asText();
        List<Double> percentageList = new ArrayList<>();

        ArrayNode aggrsPaths = (ArrayNode) chartNode.get(AGGS_PATH);

        aggrsPaths.forEach(headerPath -> {
            List<JsonNode> values =  aggregationNode.findValues(headerPath.asText());
            values.stream().parallel().forEach(value -> {
                List<JsonNode> valueNodes = value.findValues(VALUE).isEmpty() ? value.findValues("doc_count") : value.findValues(VALUE);
                Double sum = valueNodes.stream().mapToDouble(o -> o.asDouble()).sum();
                if(action.equals(PERCENTAGE)){
                    percentageList.add(sum);
                } else {
                    totalValues.add(sum);
                }
            });
        });

        String symbol = chartNode.get(IResponseHandler.VALUE_TYPE).asText();
        try{
            Data data = new Data(chartName, action.equals(PERCENTAGE)? percentageValue(percentageList) : (totalValues==null || totalValues.isEmpty())? 0.0 :totalValues.stream().reduce(0.0, Double::sum), symbol);
            dataList.add(data);
        }catch (Exception e){

            logger.info("data chart name = "+chartName +" ex occurred "+e.getMessage());
        }

        return getAggregatedDto(chartNode, dataList);
    }

}
