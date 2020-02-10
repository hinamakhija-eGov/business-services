package com.ingestpipeline.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class ProducerController {

    public static final Logger LOGGER = LoggerFactory.getLogger(ProducerController.class);

    @Value("${kafka.topics.bypass.update.data}")
    private String topic;
    @Value("${kafka.topics.bypass.update.key}")
    private String topickey;
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @PostMapping("/update/publish")
    public String publish(@RequestBody String body){
        try{

            ArrayNode nodes = (ArrayNode) new ObjectMapper().readTree(body);
            for(JsonNode node : nodes ){
                LOGGER.info("single node "+node);
                kafkaTemplate.send(topic, topickey, node);

            }
            LOGGER.info("Published successfully");
            return "Published successfully";

        } catch (Exception e){
            LOGGER.error("Published failed "+ e.getMessage());
            return "Published failed: "+e.getMessage();
        }

    }


}
