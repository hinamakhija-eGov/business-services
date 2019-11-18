package com.tarento.analytics.org.service;

import java.io.IOException;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tarento.analytics.ConfigurationLoader;
import com.tarento.analytics.constant.Constants;
import com.tarento.analytics.dto.AggregateDto;
import com.tarento.analytics.dto.AggregateRequestDtoV2;
import com.tarento.analytics.dto.CummulativeDataRequestDto;
import com.tarento.analytics.dto.DashboardHeaderDto;
import com.tarento.analytics.dto.RoleDto;
import com.tarento.analytics.enums.ChartType;
import com.tarento.analytics.exception.AINException;
import com.tarento.analytics.handler.IResponseHandler;
import com.tarento.analytics.handler.ResponseHandlerFactory;
import com.tarento.analytics.service.QueryService;
import com.tarento.analytics.service.impl.QueryServiceTemplate;


@Component
public class TarentoServiceImpl implements ClientService {
	
	public static final Logger logger = LoggerFactory.getLogger(TarentoServiceImpl.class);

	@Autowired
	private QueryService queryService;

	@Autowired
	private QueryServiceTemplate queryServiceTemplate;

	@Autowired
	private ConfigurationLoader configurationLoader;

	@Autowired
	private ResponseHandlerFactory responseHandlerFactory;

	@Override
	public AggregateDto getAggregatedData(AggregateRequestDtoV2 request, List<RoleDto> roles) throws AINException, IOException {
		// Read visualization Code
		String chartId = request.getVisualizationCode();

		ObjectNode aggrObjectNode = JsonNodeFactory.instance.objectNode();
		ObjectNode nodes = JsonNodeFactory.instance.objectNode();

		// Load Chart API configuration to Object Node for easy retrieval later
		ObjectNode node = configurationLoader.get(Constants.ConfigurationFiles.CHART_API_CONFIG);
		ChartType chartType = ChartType.fromValue(node.get(chartId).get(Constants.JsonPaths.CHART_TYPE).asText());
		ArrayNode queries = (ArrayNode) node.get(chartId).get(Constants.JsonPaths.QUERIES);
		queries.forEach(query -> {
			String module = query.get(Constants.JsonPaths.MODULE).asText();
			if(request.getModuleLevel().equals(Constants.Modules.HOME_REVENUE) || 
					request.getModuleLevel().equals(Constants.Modules.HOME_SERVICES) ||
					query.get(Constants.JsonPaths.MODULE).asText().equals(Constants.Modules.COMMON) ||
					request.getModuleLevel().equals(module)) {
				
				String indexName = query.get(Constants.JsonPaths.INDEX_NAME).asText();
				ObjectNode objectNode = queryService.getChartConfigurationQuery(request, query, indexName); 
				try {
					JsonNode aggrNode = queryServiceTemplate.search(indexName,objectNode.toString());
					nodes.set(indexName,aggrNode.get(Constants.JsonPaths.AGGREGATIONS));
				}catch (Exception e) {
					logger.error("Encountered an Exception while Executing the Query : " + e.getMessage());
				}
				aggrObjectNode.set(Constants.JsonPaths.AGGREGATIONS, nodes);

			}
		});
		IResponseHandler responseHandler = responseHandlerFactory.getInstance(chartType);
		AggregateDto aggregateDto = new AggregateDto();
		if(aggrObjectNode.fields().hasNext()){
			aggregateDto = responseHandler.translate(chartId, aggrObjectNode);
		}
		return aggregateDto;
	}

	@Override
	public List<DashboardHeaderDto> getHeaderData(CummulativeDataRequestDto requestDto, List<RoleDto> roles) throws AINException {
		// TODO Auto-generated method stub
		return null;
	}

	

}
