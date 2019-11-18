package com.tarento.analytics.controllers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.tarento.analytics.dto.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.ServletWebRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.tarento.analytics.constant.Constants;
import com.tarento.analytics.constant.ErrorCode;
import com.tarento.analytics.exception.AINException;
import com.tarento.analytics.org.service.ClientService;
import com.tarento.analytics.service.MetadataService;
import com.tarento.analytics.utils.PathRoutes;
import com.tarento.analytics.utils.ResponseGenerator;

@RestController
@RequestMapping(PathRoutes.DashboardApi.DASHBOARD_ROOT_PATH)
public class DashboardController {

	public static final Logger logger = LoggerFactory.getLogger(DashboardController.class);

	@Autowired
	private MetadataService metadataService;


    @Autowired
	private ClientService clientService;

	@GetMapping(value = PathRoutes.DashboardApi.TEST_PATH, produces = MediaType.APPLICATION_JSON_VALUE)
	public String getTest() throws JsonProcessingException {
		return ResponseGenerator.successResponse("success");

	}

	//TODO: intgrate with user Auth
	@RequestMapping(value = PathRoutes.DashboardApi.GET_CHART_V2, method = RequestMethod.POST)
	public ResponseEntity getVisualizationChartV2(@RequestBody RequestDtoV2 requestDto, @RequestHeader(value = "x-user-info", required = false) String xUserInfo, ServletWebRequest request) {

		/*logger.info("Request Detail:" + requestDto);
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		UserDto user = gson.fromJson(xUserInfo, UserDto.class);*/

		// TODO: move to userAuth helper
		UserDto user = new UserDto();
		logger.info("user"+xUserInfo);
		
		AggregateRequestDtoV2 requestInfo = requestDto.getAggregationRequestDto();
		Map<String, Object> headers = requestDto.getHeaders();
		//requestInfo.getFilters().putAll(headers);
		ResponseEntity responseEntity = null;
		try {
			if (headers.isEmpty() || headers.get("tenantId") == null) {
				logger.error("Please provide header details");
				responseEntity = new ResponseEntity(new ResponseDto(HttpStatus.BAD_REQUEST.value(), "header or tenantId is missing", null), HttpStatus.BAD_REQUEST);

			} else {
				// To be removed once the development is complete
				if(StringUtils.isBlank(requestInfo.getModuleLevel())) {
					requestInfo.setModuleLevel(Constants.Modules.HOME_REVENUE);
				}

				AggregateDto responseData = clientService.getAggregatedData(requestInfo, user.getRoles());
				responseEntity = new ResponseEntity(new ResponseDto(HttpStatus.OK.value(), "success", responseData), HttpStatus.OK);
			}

		} catch (Exception e) {
			logger.error("error while executing api getVisualizationChart", e.getMessage());
			responseEntity = new ResponseEntity(new ResponseDto(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getMessage(), null), HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return responseEntity;
	}

	//TODO: integrate with user Auth
	@RequestMapping(value = PathRoutes.DashboardApi.GET_DASHBOARD_CONFIG + "/{dashboardId}", method = RequestMethod.GET)
	public ResponseEntity getDashboardConfiguration(@PathVariable String dashboardId, @RequestParam(value="catagory", required = false) String catagory, @RequestHeader(value = "x-user-info", required = false) String xUserInfo) {

		//Gson gson = new GsonBuilder().setPrettyPrinting().create();
		//TODO- to remove the hard coded stuffs once integrated and comes from

		try {
			UserDto user = new UserDto();
			user.setId(new Long("10007"));
			user.setOrgId("1");
			user.setCountryCode("");
			RoleDto role = new RoleDto();
			role.setId(new Long("6"));
			role.setName("HR User");
			List<RoleDto> roles = new ArrayList<>();
			roles.add(role);
			user.setRoles(roles);

			if(null == catagory || catagory.isEmpty())
				return new ResponseEntity(new ResponseDto(HttpStatus.BAD_REQUEST.value(), "invalid catagory!", null), HttpStatus.BAD_REQUEST);

			Object response = metadataService.getDashboardConfiguration(dashboardId, catagory, user.getRoles());
			return new ResponseEntity(new ResponseDto(HttpStatus.OK.value(), "success", response), HttpStatus.OK);

		} catch (Exception e) {
			return new ResponseEntity(new ResponseDto(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getMessage(), null), HttpStatus.INTERNAL_SERVER_ERROR);

		}
	}

}
