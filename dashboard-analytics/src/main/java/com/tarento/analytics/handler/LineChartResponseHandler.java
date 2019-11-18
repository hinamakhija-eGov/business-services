package com.tarento.analytics.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tarento.analytics.ConfigurationLoader;
import com.tarento.analytics.dto.AggregateDto;
import com.tarento.analytics.dto.Data;
import com.tarento.analytics.dto.Plot;
import com.tarento.analytics.enums.ChartType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
/**
 * This handles ES response for single index, multiple index to represent data as line chart
 * Creates plots by merging/computing(by summation) index values for same key
 * AGGS_PATH : this defines the path/key to be used to search the tree
 * VALUE_TYPE : defines the data type for the value formed, this could be amount, percentage, number
 *
 */
@Component
public class LineChartResponseHandler implements IResponseHandler {
    public static final Logger logger = LoggerFactory.getLogger(LineChartResponseHandler.class);

    @Autowired
    ConfigurationLoader configurationLoader;

    @Override
    public AggregateDto translate(String chartId, ObjectNode aggregations) throws IOException {

        List<Data> dataList = new LinkedList<>();

        JsonNode aggregationNode = aggregations.get(AGGREGATIONS);
        ObjectNode configNode = configurationLoader.get(API_CONFIG_JSON);
        JsonNode chartNode = configNode.get(chartId);
        String symbol = chartNode.get(IResponseHandler.VALUE_TYPE).asText();
        ArrayNode aggrsPaths = (ArrayNode) chartNode.get(IResponseHandler.AGGS_PATH);
        Set<String> plotKeys = new HashSet<>();

        aggrsPaths.forEach(headerPath -> {
            List<JsonNode> aggrNodes = aggregationNode.findValues(headerPath.asText());

            Map<String, Double> plotMap = new LinkedHashMap<>();
            List<Double> totalValues = new ArrayList<>();
            aggrNodes.stream().parallel().forEach(aggrNode -> {
                if (aggrNode.findValues(IResponseHandler.BUCKETS).size() > 0) {

                    ArrayNode buckets = (ArrayNode) aggrNode.findValues(IResponseHandler.BUCKETS).get(0);
                    buckets.forEach(bucket -> {
                        String bkey = bucket.findValue(IResponseHandler.KEY).asText();
                        String key = getWeekOfMonthOfYear(bkey);

                        plotKeys.add(key);
                        double value = bucket.findValue(IResponseHandler.VALUE).asDouble();
                        plotMap.put(key, plotMap.get(key) == null ? new Double("0") + value : plotMap.get(key) + value);
                        totalValues.add(value);
                    });
                }
            });
            List<Plot> plots = plotMap.entrySet().stream().map(e -> new Plot(e.getKey(), e.getValue(), symbol)).collect(Collectors.toList());
            try{
                Data data = new Data(headerPath.asText(), (totalValues==null || totalValues.isEmpty()) ? 0.0 : totalValues.stream().reduce(0.0, Double::sum), symbol);
                data.setPlots(plots);
                dataList.add(data);
            }catch (Exception e) {
                logger.info(" Legend/Header "+headerPath.asText() +" exception occurred "+e.getMessage());
            }


        });

        dataList.forEach(data -> {
            appendMissingPlot(plotKeys, data, symbol);
        });
        return getAggregatedDto(chartNode, dataList);
    }

    private String getWeekOfMonthOfYear(String epocString) {
        try {
            long epoch = Long.parseLong( epocString );
            Date expiry = new Date( epoch );
            Calendar cal = Calendar.getInstance();
            cal.setTime(expiry);
            
            String day = String.valueOf(cal.get(Calendar.DATE)); 
            String month = monthNames(cal.get(Calendar.MONTH)+1); 
            String dayMonth = day.concat("-").concat(month); 
            String year =  ""+cal.get(Calendar.YEAR);
            String weekMonth = "Week " + cal.get(Calendar.WEEK_OF_YEAR)  + " : " +  dayMonth;//+" of Month "+ (cal.get(Calendar.MONTH) + 1);
            return dayMonth;
        } catch (Exception e) {
            return epocString;
        }
    }
    
    private String monthNames(int month) { 
    	if(month == 1)  
    		return "Jan";
    	else if(month == 2)  
    		return "Feb";
    	else if(month == 3)  
    		return "Mar";
    	else if(month == 4)  
    		return "Apr";
    	else if(month == 5)  
    		return "May";
    	else if(month == 6)  
    		return "Jun";
    	else if(month == 7)  
    		return "Jul";
    	else if(month == 8)  
    		return "Aug";
    	else if(month == 9)  
    		return "Sep";
    	else if(month == 10)  
    		return "Oct";
    	else if(month == 11)  
    		return "Nov";
    	else if(month == 12)  
    		return "Dec";
    	else 
    		return "Month";
    }
}
