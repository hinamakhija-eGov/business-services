package com.ingestpipeline.consumer;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.ingestpipeline.producer.IngestProducer;
import com.ingestpipeline.service.EnrichmentService;
import com.ingestpipeline.service.IESService;
import com.ingestpipeline.util.Constants;

@Service
public class EnrichmentConsumer implements KafkaConsumer {

	public static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentConsumer.class);
	private static final String ERROR_INTENT = "DataError";
	private static final String INTENT = "enrichment" ;
	
	@Value("${es.push.direct}")
	private Boolean esPushDirect;
	
	@Value("${kafka.topics.enriched.data}")
	private String enrichedDataTopic;
	
	@Value("${kafka.topics.enriched.key}")
	private String enrichedDataKey;
	
	@Autowired
	private EnrichmentService enrichmentService;
	
	@Autowired
	private IngestProducer ingestProducer;
	
	@Autowired
	private IESService elasticService;

	@KafkaListener(id = INTENT, groupId = INTENT, topics = { Constants.KafkaTopics.TRANSFORMED_DATA}, containerFactory = Constants.BeanContainerFactory.INCOMING_KAFKA_LISTENER)
	public void processMessage(final Map incomingData,
			@Header(KafkaHeaders.RECEIVED_TOPIC) final String topic) {
		LOGGER.info("##KafkaMessageAlert## : key:" + topic + ":" + "value:" + incomingData.size());

	        for(Object key : incomingData.keySet()){
			push((Map)incomingData.get(key));
		}

	}

	private void push(Map incomingData) {

		try {
			Map updatedIncomingData = enrichmentService.enrichData(incomingData);

			if(esPushDirect) {
				elasticService.push(incomingData);
			} else {
				ingestProducer.pushToPipeline(incomingData, enrichedDataTopic, enrichedDataKey);
			}
			if(updatedIncomingData == null) {
				ingestProducer.pushToPipeline(incomingData, ERROR_INTENT, ERROR_INTENT);
			}
		} catch (final Exception e) {
			e.printStackTrace();
			LOGGER.error("Exception Encountered while processing the received message : " + e.getMessage());
		}

	}

}
