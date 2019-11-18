package com.ingestpipeline;

import com.ingestpipeline.controller.RestApiController;
import com.ingestpipeline.model.CollectionDomainConfig;
import com.ingestpipeline.util.ConfigLoader;
import com.ingestpipeline.util.ReadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * The App Startup Runner runs on the start of the application as it implements Application Runner
 * This will be responsible to load the configurations which are necessary for the Enrichment in the Data Pipeline 
 * Resources and Domain level configurations are fetched here. 
 * @author Pritha
 *
 */
@Component
public class AppStartupRunner implements ApplicationRunner {

    private static Logger logger = LoggerFactory.getLogger(AppStartupRunner.class);

	@Autowired
	ConfigLoader configLoader;
	@Autowired
    CollectionDomainConfig collectionDomainConfig;
	
	  @Autowired ReadUtil readutil;
	 
	  @Autowired RestApiController restApiController;

    @SuppressWarnings("static-access")
	@Override
    public void run(ApplicationArguments args) throws Exception {
    	logger.info("On Boot starts loading: config resources ");
		configLoader.loadResources();
        collectionDomainConfig.loadCollectionDomains();
    }
}
