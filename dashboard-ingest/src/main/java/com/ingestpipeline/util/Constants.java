package com.ingestpipeline.util;

/**
 * Constants which are with respect to the Ingest App
 * 
 * @author Darshan Nagesh
 *
 */

public interface Constants {
	public interface Paths {
		final String ELASTIC_PUSH_CONTROLLER_PATH = "/ingest";
		final String SAVE = "/save";
		final String UPLOAD = "/upload";
		final String Targets = "/targets";
		final String Collections = "/getCollections";
		final String ES_INDEX = "/migrate/{indexName}";
	}
	
	public interface Qualifiers { 
		final String INGEST_SERVICE = "ingestService"; 
		final String VALIDATOR_SERVICE = "validatorService";
		final String TRANSFORM_SERVICE = "transformService";
		final String ENRICHMENT_SERVICE = "enrichmentService"; 
	}

	public static String SUCCESS = "success";
	public static int UNAUTHORIZED_ID = 401;
	public static int SUCCESS_ID = 200;
	public static int FAILURE_ID = 320;
	public static String UNAUTHORIZED = "Invalid credentials. Please try again.";
	public static String PROCESS_FAIL = "Process failed, Please try again.";
	public static String DATE_FORMAT = "yyyy.MM.dd G 'at' HH:mm:ss z";
	public static String INDIAN_TIMEZONE = "IST";

	public static String ALLOWED_METHODS_GET = "GET";
	public static String ALLOWED_METHODS_POST = "POST";

	public interface KafkaTopics {
		public static final String INGEST_DATA = "ingestData";
		public static final String VALID_DATA = "validData";
		public static final String TRANSFORMED_DATA = "transformedData";
		public static final String ERROR_INTENT = "DataError";
	}
	
	public interface BeanContainerFactory { 
		public static final String INCOMING_KAFKA_LISTENER = "incomingKafkaListenerContainerFactory"; 
	}
	
	public interface PipelineRules {
		public static final String VALIDATE_DATA = "VALIDATE";
		public static final String TRANSFORM_DATA = "TRANSFORM";
		public static final String ENRICH_DATA = "ENRICH";
	}

	public static String DATA_CONTEXT = "dataContext";
	public static String DATA_CONTEXT_VERSION = "dataContextVersion";
	public static String DATA_OBJECT = "dataObject";

	public static String ERROR_IN_PIPEINE = "errorPipeline";
	
	public static int HEADER_ROW = 1;
	public static String MUNICIPAL_CORPORATIONS = "Municipal Corporations";
	public static String ES_INDEX_COLLECTION = "collectionsindex-v1";
	public static String ES_INDEX_BILLING = "billingservice";
}
