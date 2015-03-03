package com.github.ningg.flume.sink;

import org.apache.flume.source.ExecSourceConfigurationConstants;

public class BaseKafkaSinkConfigurationConstants {

	public static final String PROPERTY_PREFIX = "kafka";

    /**
     * Topic for the events send to Kafka cluster.
     */
    public static final String CONFIG_TOPIC = "topic";
    public static final String DEFAULT_TOPIC = "default-flume-topic";
    
    
    /**
     * Charset for the events from the Flume Source, 
     * when use the charset property, it should be set the same with the CHARSET of {@link ExecSourceConfigurationConstants}  
     * 
     * Also see {@link ExecSourceConfigurationConstants#CHARSET}
     */
    public static final String CONFIG_CHARSET = "charset";
    public static final String DEFAULT_CHARSET = "UTF-8";
}
