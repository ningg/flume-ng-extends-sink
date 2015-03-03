package com.github.ningg.flume.sink;

import kafka.producer.KeyedMessage;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Refer to https://github.com/thilinamb/flume-ng-kafka-sink
 * 
 * @author 	Ning Guo
 * @Email	guoning.gn@gmail.com
 *
 */

public class OneToOneKafkaSink extends BaseKafkaSink{

	private static final Logger logger = LoggerFactory.getLogger(OneToOneKafkaSink.class);
	
	@Override
	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;
		String eventTopic = super.getTopic();
		String eventKey = null;
		
		try{
			transaction.begin();
			event = channel.take();
			
			if(event != null){
				// get the message body with the configured charset
				String charset = super.getContext().getString(BaseKafkaSinkConfigurationConstants.CONFIG_CHARSET, BaseKafkaSinkConfigurationConstants.DEFAULT_CHARSET);
				String eventBody = new String(event.getBody(), charset);
				
				// log the event for debugging
				if (logger.isDebugEnabled()){
					logger.debug("{Event} " + eventBody);
				}
				
				logger.info("event body is: " + eventBody);
				
				// create a message
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(eventTopic, eventKey, eventBody);
				
				// publish 
				super.getProducer().send(data);
			} else {
				// No event found, request back-off semantics from the sink runner
				result = Status.BACKOFF;
			}
			// publishing is successful. Commit.
			transaction.commit();
		} catch (Exception ex){
			transaction.rollback();
			String errorMsg = "Failed to publish event: " + event ;
			logger.error(errorMsg);
			throw new EventDeliveryException(errorMsg, ex);
		} finally {
			transaction.close();
		}
		
		return result;
	}

}
