package com.ingestpipeline.controller;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

import com.ingestpipeline.model.TargetData;
import com.ingestpipeline.repository.TargetDataDao;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import com.bazaarvoice.jolt.modifier.DataType.MAP;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ingestpipeline.model.IncomingData;
import com.ingestpipeline.service.IngestService;
import com.ingestpipeline.util.Constants;
import com.ingestpipeline.util.JSONUtil;
import com.ingestpipeline.util.JSONUtils;
import com.ingestpipeline.util.ReadUtil;
import com.ingestpipeline.service.ElasticService;

@RestController
@RequestMapping(Constants.Paths.ELASTIC_PUSH_CONTROLLER_PATH)
public class RestApiController {

	public static final Logger logger = LoggerFactory.getLogger(RestApiController.class);

	@Autowired
	IngestService ingestService;

	@Autowired
	TargetDataDao targetDataDao;

	@Autowired
	private KafkaListenerEndpointRegistry endPointRegistry;

	@Autowired
	private ElasticService elasticService;

	/**
	 * This API use to pause a active kafka consumer
	 *
	 * @param consumerId kafka consumer identifier
	 * @return
	 */
	@RequestMapping(value = "/pause/{consumerId}", method = RequestMethod.GET)
	public Boolean pauseConsumer(@PathVariable final String consumerId) {
		endPointRegistry.getListenerContainer(consumerId).pause();
		return Boolean.TRUE;
	}

	/**
	 * This API is to resume a paused kafka consumer
	 *
	 * @param consumerId kafka consumer identifier
	 * @return
	 */
	@RequestMapping(value = "/resume/{consumerId}", method = RequestMethod.GET)
	public Boolean resumeConsumer(@PathVariable final String consumerId) {
		endPointRegistry.getListenerContainer(consumerId).resume();
		return Boolean.TRUE;
	}

	/**
	 * This API receives the Transaction Details JSON Request and passes it on to
	 * the Service Layer for further process of persisting into elastic search
	 * database
	 * 
	 * @param transaction
	 * @return
	 */
	@RequestMapping(value = Constants.Paths.SAVE, method = RequestMethod.POST)
	public ResponseEntity<?> save(@RequestBody IncomingData incomingData) {
		logMyTime();
		Boolean status = ingestService.ingestToPipeline(incomingData);
		if (status) {
			return new ResponseEntity<String>(HttpStatus.CREATED);
		}
		return new ResponseEntity<String>(HttpStatus.SERVICE_UNAVAILABLE);
	}

	/**
	 * This API use to provide response for external data upload
	 * 
	 * @param get file
	 * @author Rahul
	 * @throws Exception
	 */
	@RequestMapping(value = Constants.Paths.Targets, method = RequestMethod.GET)
	public String getTargets() throws Exception {
		List<TargetData> datas = (List<TargetData>) targetDataDao.findAll();
		String response = JSONUtil.getJsonString(new ObjectMapper(), datas);
		return response;
	}

	/**
	 * This API use to import external data upload Request
	 * 
	 * @param upload file
	 * @author Rahul
	 * @throws Exception
	 */
	@RequestMapping(value = Constants.Paths.UPLOAD, method = RequestMethod.POST)
	public ResponseEntity<?> handleFileUpload(@RequestParam("file") MultipartFile file) throws Exception {
		Boolean status = ingestService.ingestToPipeline(getWrapper(file));
		if (status) {
			return new ResponseEntity<String>(HttpStatus.CREATED);
		}
		return new ResponseEntity<String>(HttpStatus.SERVICE_UNAVAILABLE);
	}

	public IncomingData getWrapper(MultipartFile file) throws Exception {
		JSONArray jsonArray = new JSONArray();
		jsonArray = ReadUtil.getFiletoDirectory(file);
		IncomingData incomingData = new IncomingData();
		incomingData.setDataContext("target");
		incomingData.setDataContextVersion("v1");
		Iterator itr = jsonArray.iterator();
		List<Map> list = new ArrayList<Map>();
		while (itr.hasNext()) {
			JSONObject obj = (JSONObject) itr.next();
			Map mapJ = new Gson().fromJson(obj.toString(), Map.class);
			list.add(mapJ);
		}
		incomingData.setDataObject(list);
		return incomingData;
	}

	/**
	 * This API to post documents from ES index
	 * 
	 * @param post documents
	 * @author Rahul
	 * @throws Exception
	 */
	@RequestMapping(value = Constants.Paths.ES_INDEX, method = RequestMethod.POST)
	public ResponseEntity<?> migrateIndex(@PathVariable String indexName) throws Exception {
		String index = null, queryString = null, dataContext = null;
		if (indexName.equals(Constants.ES_INDEX_COLLECTION)) {
			index = Constants.ES_INDEX_COLLECTION;
			queryString = elasticService.getSearchQueryCollection();
			dataContext = "collection";
		} else if (indexName.equals(Constants.ES_INDEX_BILLING)) {
			index = Constants.ES_INDEX_BILLING;
			queryString = elasticService.getSearchQueryBilling();
			dataContext = "billing";
		} else {
			index = "notDefinedIndex";
			queryString = "noquery";
		}
		Boolean status = false;
		if (!index.equals("notDefinedIndex")) {
			Map<String, List<Object>> doc_Map = elasticService.searchIndex(index, queryString);
			for (Map.Entry<String, List<Object>> entry : doc_Map.entrySet()) {
				for (int i = 0; i < entry.getValue().size(); i++) {
					status = ingestService
							.ingestToPipeline(setIncomingData(dataContext, "v1", entry.getValue().get(i)));
				}
			}
		}
		if (status) {
			return new ResponseEntity<String>(HttpStatus.CREATED);
		} else if (index.equals("notDefinedIndex")) {
			return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<String>(HttpStatus.SERVICE_UNAVAILABLE);
	}

	private IncomingData setIncomingData(String index, String version, Object documentValue) {
		IncomingData incomingData = new IncomingData();
		incomingData.setDataContext(index);
		incomingData.setDataContextVersion(version);
		incomingData.setDataObject(documentValue);
		return incomingData;
	}

	private void logMyTime() {
		logger.info("System Time is : " + new Date());
		SimpleDateFormat sd = new SimpleDateFormat(Constants.DATE_FORMAT);
		Date date = new Date();
		sd.setTimeZone(TimeZone.getTimeZone(Constants.INDIAN_TIMEZONE));
		logger.info("Time at timezone IST : " + sd.format(date));
	}
}