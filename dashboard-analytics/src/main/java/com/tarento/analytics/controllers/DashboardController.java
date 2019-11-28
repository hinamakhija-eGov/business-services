package com.tarento.analytics.controllers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
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
import com.tarento.analytics.dto.AggregateRequestDto;
import com.tarento.analytics.dto.AggregateRequestDtoV3;
import com.tarento.analytics.dto.RequestDto;
import com.tarento.analytics.dto.RequestDtoV3;
import com.tarento.analytics.dto.RoleDto;
import com.tarento.analytics.dto.UserDto;
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

/*
	//TODO: intgrate with user Auth
	@RequestMapping(value = PathRoutes.DashboardApi.GET_CHART_V2 + /{chartId}, method = RequestMethod.POST)
	public ResponseEntity getVisualizationChartV2(@RequestBody RequestDtoV2 requestDto, @RequestHeader(value = "x-user-info", required = false) String xUserInfo, ServletWebRequest request) {

		*/
/*logger.info("Request Detail:" + requestDto);
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		UserDto user = gson.fromJson(xUserInfo, UserDto.class);*//*


		// TODO: move to userAuth helper
		UserDto user = new UserDto();
		logger.info("user"+xUserInfo);
		
		AggregateRequestDtoV2 requestInfo = requestDto.getAggregationRequestDto();
		requestInfo.setVisualizationCode(chartId);
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
*/

	@RequestMapping(value = PathRoutes.DashboardApi.GET_DASHBOARD_CONFIG + "/{dashboardId}", method = RequestMethod.GET)
	public String getDashboardConfiguration(@PathVariable String dashboardId, @RequestParam(value="catagory", required = false) String catagory, @RequestHeader(value = "x-user-info", required = false) String xUserInfo)
			throws AINException, IOException {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
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
		//gson.fromJson(xUserInfo, UserDto.class);

		return ResponseGenerator.successResponse(metadataService.getDashboardConfiguration(dashboardId, catagory, user.getRoles()));
	}

	@RequestMapping(value = PathRoutes.DashboardApi.GET_CHART_V2, method = RequestMethod.POST)
	public String getVisualizationChartV2( @RequestBody RequestDto requestDto, @RequestHeader(value = "x-user-info", required = false) String xUserInfo, ServletWebRequest request)
			throws IOException {

		/*logger.info("Request Detail:" + requestDto);
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		UserDto user = gson.fromJson(xUserInfo, UserDto.class);*/

		UserDto user = new UserDto();
		logger.info("user"+xUserInfo);

		//Getting the request information only from the Full Request
		AggregateRequestDto requestInfo = requestDto.getAggregationRequestDto();
		Map<String, Object> headers = requestDto.getHeaders();
		//requestInfo.getFilters().putAll(headers);
		String response = "";
		try {
			if (headers.isEmpty()) {
				logger.error("Please provide header details");
				throw new AINException(ErrorCode.ERR320, "header is missing");
			}
			if (headers.get("tenantId") == null) {
				logger.error("Please provide tenant ID details");
				throw new AINException(ErrorCode.ERR320, "tenant is missing");

			}

			// To be removed once the development is complete
			if(StringUtils.isBlank(requestInfo.getModuleLevel())) {
				requestInfo.setModuleLevel(Constants.Modules.HOME_REVENUE);
			}

			Object responseData = clientService.getAggregatedData(requestInfo, user.getRoles());
			response = ResponseGenerator.successResponse(responseData);
			/*ValidationReport report = validationManager.validateVisualizationRequestV2(requestDto);
			if (report.isSuccess()) {
				Object responseData = clientService.getAggregatedDataV2(requestInfo, user.getRoles());
				response = ResponseGenerator.successResponse(responseData);
			}else{
				throw new AINException(ErrorCode.ERR320, report.getField()+","+ report.getValue());
			}*/
		} catch (AINException e) {
			logger.error("error while executing api getVisualizationChart");
			response = ResponseGenerator.failureResponse(e.getErrorCode(), e.getErrorMessage());
		}
		return response;
	}
	
	@RequestMapping(value = PathRoutes.DashboardApi.GET_CHART_V3, method = RequestMethod.POST)
	public String getVisualizationChartV3(@RequestBody RequestDtoV3 requestDtoV3, @RequestHeader(value = "x-user-info", required = false) String xUserInfo, ServletWebRequest request)
			throws IOException {

		/*logger.info("Request Detail:" + requestDto);
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		UserDto user = gson.fromJson(xUserInfo, UserDto.class);*/

		UserDto user = new UserDto();
		logger.info("user"+xUserInfo);

		//Getting the request information only from the Full Request
		AggregateRequestDtoV3 requestInfoV3 = requestDtoV3.getAggregationRequestDto();
		Map<String, Object> headers = requestDtoV3.getHeaders();
		//requestInfo.getFilters().putAll(headers);
		String response = "";
		try {
			if (headers.isEmpty()) {
				logger.error("Please provide header details");
				throw new AINException(ErrorCode.ERR320, "header is missing");
			}
			if (headers.get("tenantId") == null) {
				logger.error("Please provide tenant ID details");
				throw new AINException(ErrorCode.ERR320, "tenant is missing");
			}
			// To be removed once the development is complete
			if(StringUtils.isBlank(requestInfoV3.getModuleLevel())) {
				requestInfoV3.setModuleLevel(Constants.Modules.HOME_REVENUE);
			}

			List<Object> responseDataList = new ArrayList<>(); 
			if(requestInfoV3 !=null && requestInfoV3.getVisualizations() != null && requestInfoV3.getVisualizations().size() > 0) {
				for (int i = 0; i < requestInfoV3.getVisualizations().size(); i++) {
					AggregateRequestDto requestInfo = new AggregateRequestDto(requestInfoV3,
							requestInfoV3.getVisualizations().get(i).getType(), requestInfoV3.getVisualizations().get(i).getCode());
					Object responseData = clientService.getAggregatedData(requestInfo, user.getRoles());
					responseDataList.add(responseData); 
				}
				
			}
			response = ResponseGenerator.successResponse(responseDataList);
		} catch (AINException e) {
			logger.error("error while executing api getVisualizationChart");
			response = ResponseGenerator.failureResponse(e.getErrorCode(), e.getErrorMessage());
		}
		return response;
	}


	/*//TODO: integrate with user Auth
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
*/
	@RequestMapping(value = PathRoutes.DashboardApi.TARGET_DISTRICT_ULB, method = RequestMethod.GET,produces = "application/json")
	public ResponseEntity<String> getTargetDistrictUlb() throws Exception {
		JSONArray response = metadataService.getTargetDistrict();
		return new ResponseEntity<>(response.toString(), HttpStatus.OK);
	}


}
