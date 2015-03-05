package com.github.ningg.flume.sink;

import java.util.Map;
import java.util.Properties;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Sink refers to flume-ng-kafka-sink(https://github.com/thilinamb/flume-ng-kafka-sink)
 * 
 * Kafka 0.8.2.0: New Producer API
 * 
 * @author ningg (http://ningg.github.com)
 *
 */
public abstract class BaseKafkaSinkNewProducer extends AbstractSink implements Configurable{

	private static final Logger logger = LoggerFactory.getLogger(BaseKafkaSinkNewProducer.class);
	private Properties producerProps;
	private KafkaProducer<String, String> producer;
	private String topic;
	private Context context;
	
	@Override
	public abstract Status process() throws EventDeliveryException;
	
	
	@Override
	public void configure(Context context) {
		this.context = context;
		
		// read the properties for Kafka Producer
		// any property that has the prefix "kafka" in the key will be considered
		// as a property that is passed when instantiating the producer.
		Map<String, String> params = context.getParameters();
		producerProps = new Properties();
		for(String key : params.keySet()){
			String value = params.get(key).trim();
			key = key.trim();
			if(key.startsWith(BaseKafkaSinkConfigurationConstants.PROPERTY_PREFIX)){
				key = key.substring(BaseKafkaSinkConfigurationConstants.PROPERTY_PREFIX.length() + 1, key.length());
				producerProps.put(key, value);
				if (logger.isDebugEnabled()){
					logger.debug("Reading a Kafka Producer Property: [key]: " + key + ", [value]: " + value);
				}
			}
		}
		
		topic = context.getString(BaseKafkaSinkConfigurationConstants.CONFIG_TOPIC, BaseKafkaSinkConfigurationConstants.DEFAULT_TOPIC);
		if(topic.equals(BaseKafkaSinkConfigurationConstants.DEFAULT_TOPIC)){
			logger.warn("The Properties 'metadata.extractor' or 'topic' is not set. Using the default topic name: " +
					BaseKafkaSinkConfigurationConstants.DEFAULT_TOPIC);
		}else{
			logger.info("Using the static topic: " + topic);
		}
	}
	
	@Override
	public synchronized void start(){
		producer = new KafkaProducer<String, String>(producerProps);
		super.start();
	}
	
	@Override
	public synchronized void stop(){
		producer.close();
		super.stop();
	}

	public synchronized Properties getProducerProps() {
		return producerProps;
	}

	public synchronized KafkaProducer<String, String> getProducer() {
		return producer;
	}

	public synchronized String getTopic() {
		return topic;
	}

	public synchronized Context getContext() {
		return context;
	}

	
}
