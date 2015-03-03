package com.github.ningg.flume.sink;

import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Sink refers to flume-ng-kafka-sink(https://github.com/thilinamb/flume-ng-kafka-sink)
 * @author ningg (http://ningg.github.com)
 *
 */
public abstract class BaseKafkaSink extends AbstractSink implements Configurable{

	private static final Logger logger = LoggerFactory.getLogger(BaseKafkaSink.class);
	private Properties producerProps;
	private Producer<String, String> producer;
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
		ProducerConfig config = new ProducerConfig(producerProps);
		producer = new Producer<String, String>(config);
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

	public synchronized Producer<String, String> getProducer() {
		return producer;
	}

	public synchronized String getTopic() {
		return topic;
	}

	public synchronized Context getContext() {
		return context;
	}

	
}
