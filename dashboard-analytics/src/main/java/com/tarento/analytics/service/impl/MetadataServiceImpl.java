package com.tarento.analytics.service.impl;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tarento.analytics.ConfigurationLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.tarento.analytics.dto.RoleDto;
import com.tarento.analytics.exception.AINException;
import com.tarento.analytics.service.MetadataService;

@Service("metadataService")
public class MetadataServiceImpl implements MetadataService {

	public static final Logger logger = LoggerFactory.getLogger(MetadataServiceImpl.class);

	 @Autowired
	 private ConfigurationLoader configurationLoader;
	 @Autowired
	 private ObjectMapper objectMapper;

	@Override
	public ArrayNode getDashboardConfiguration(String dashboardId, String catagory, List<RoleDto> roleIds) throws AINException, IOException {

		ObjectNode configNode = configurationLoader.get(ConfigurationLoader.ROLE_DASHBOARD_CONFIG);
		ArrayNode rolesArray = (ArrayNode) configNode.findValue("roles");
		ArrayNode dbArray = JsonNodeFactory.instance.arrayNode();

		rolesArray.forEach(role -> {
			Object roleId = roleIds.stream().filter(x -> role.get("roleId").asLong() == (x.getId())).findAny().orElse(null);
			System.out.println("roleId = "+roleId);

			if (null != roleId) {
				role.get("dashboards").forEach(dashboard -> {
					ObjectNode copyDashboard = dashboard.deepCopy();
					ArrayNode visArray = JsonNodeFactory.instance.arrayNode();
					if(catagory != null) {
						copyDashboard.get("visualizations").forEach(visual ->{
							if(visual.get("name").asText().equalsIgnoreCase(catagory)){
								visArray.add(visual);
							}
						});
						copyDashboard.set("visualizations", visArray);
					}
					if(dashboard.get("id").asText().equalsIgnoreCase(dashboardId)){
						dbArray.add(copyDashboard);
					}
				});
			}
		});

		//List<Dashboard> dbs = objectMapper.readValue(dbArray.toString(), new TypeReference<List<Dashboard>>() {});
		return dbArray;
	}

}
