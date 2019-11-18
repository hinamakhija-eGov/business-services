package com.ingestpipeline.service;

import java.io.File;

import org.springframework.stereotype.Service;

import com.ingestpipeline.model.IncomingData;

@Service
public interface IngestService {
	
	Boolean ingestToPipeline(Object incomingData);
	
}
