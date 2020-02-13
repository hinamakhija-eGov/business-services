package com.ingestpipeline.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ingestpipeline.service.IESService;
import com.ingestpipeline.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.net.URLEncoder;

@Component
public class UpdateConsumer {


    public static final Logger LOGGER = LoggerFactory.getLogger(UpdateConsumer.class);

    @Autowired
    private ObjectMapper mapper;
    @Autowired
    private IESService elasticService;

    @KafkaListener(topics = "${kafka.topics.bypass.update.data}" , containerFactory = Constants.BeanContainerFactory.INCOMING_KAFKA_LISTENER)
    public void processMessage(Map data,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) final String topic) {
        LOGGER.info("##KafkaMessageAlert## : key:" + topic + ":" + "value:" + data.size());
        try {

            LOGGER.info("data ### "+data.toString());
            String index =  data.get("_index").toString();
            String type =  data.get("_type").toString();
            JsonNode sourceNode = mapper.convertValue(data.get("_source"), JsonNode.class);
            //String id = data.get("_id").toString();
            String id = URLEncoder.encode(data.get("_id").toString());
            JsonNode response  = elasticService.post(index, type, id, "", sourceNode.toString());
            LOGGER.info("Updated response " + response + " for index "+ index);

        } catch (final Exception e) {
            e.printStackTrace();
            LOGGER.error("Exception Encountered while processing the received message updating posted data for topic: "+ topic +"" + e.getMessage());
        }
    }


}
