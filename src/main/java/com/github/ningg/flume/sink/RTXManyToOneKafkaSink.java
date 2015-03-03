package com.github.ningg.flume.sink;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;

import kafka.producer.KeyedMessage;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class RTXManyToOneKafkaSink extends BaseKafkaSink{

	private static final Logger logger = LoggerFactory.getLogger(RTXManyToOneKafkaSink.class);
	
	@Override
	public Status process() throws EventDeliveryException {
		
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;
		String eventTopic = super.getTopic();
		String eventKey = null;
		String eventBody = null;
		
		try{
			/**
			 *  Extract RTX JSON --> START
			 */
			
			Matcher threadIDMat = RTXRegex.getThreadIdMat();
			Matcher timeMat = RTXRegex.getTimeMat();
			Matcher senderMat = RTXRegex.getSenderMat();
			Matcher receiversMat = RTXRegex.getReceviersMat();
			
			RTXInfo rtxInfo = new RTXInfo();
			rtxInfo.clearAll();
			RTXInfoFirstEdition rtxInfoFirstEdition = new RTXInfoFirstEdition();

			SimpleDateFormat simpleDateFormat = new SimpleDateFormat(RTXRegex.RTX_DATE_FORMAT);
			String charset = super.getContext().getString(BaseKafkaSinkConfigurationConstants.CONFIG_CHARSET, BaseKafkaSinkConfigurationConstants.DEFAULT_CHARSET);
			String inputEventBody;
			Gson gson = new Gson();

			transaction.begin();
			
			
			while( (event = channel.take()) != null ){
			
				inputEventBody = new String(event.getBody(), charset);
				
				threadIDMat.reset(inputEventBody);
				timeMat.reset(inputEventBody);
				senderMat.reset(inputEventBody);
				receiversMat.reset(inputEventBody);
				
				if (threadIDMat.find()) {
					rtxInfo.clearAll();
					rtxInfo.setThreadID(threadIDMat.group(1));
				}
				
				// ThreadID is the started flag.
				if( rtxInfo.isStarted() && !rtxInfo.isFinished() ){
					if(timeMat.find()){
						// rtxInfo.setTime(simpleDateFormat.parse(timeMat.group(1)));
						rtxInfo.setTime(simpleDateFormat.parse(RTXRegex.dealWithDate(timeMat)));	// month plus 1
					}else if (senderMat.find()) {
						rtxInfo.setSender(senderMat.group(1));
					}else if (receiversMat.find()) {
						String[] recevierArray = receiversMat.group(1).split(RTXRegex.RTX_RECEIVER_SPLIT);
						rtxInfo.setReceivers(Arrays.asList(recevierArray));
					}
					
				}
				
				// Return the RTXInfo object.
				if (rtxInfo.isFinished()) {
					//eventBody = gson.toJson(rtxInfo);		// General Edition.
					rtxInfoFirstEdition.retrieveFromRTXInfo(rtxInfo);	// For First Edition.
					eventBody = gson.toJson(rtxInfoFirstEdition);
					eventKey = String.valueOf(RandomUtils.nextInt());
					
					if (logger.isDebugEnabled()) {	// Log the event for debugging.
						logger.debug("Output RTXInfo JSON is: {}", eventBody);
					}
					
					// Create a message
					KeyedMessage<String, String> data = new KeyedMessage<String, String>(eventTopic, eventKey, eventBody);
					super.getProducer().send(data);
					// rtxInfo.clearAll();	// Do not clearAll, so that can deal with the scenario: only parts of RTX unit are taken.
					break;		// Only send one Kafka event
				}
				
			}
				
			if ( (event == null) && (!rtxInfo.isClean()) ) {
				// No event found, request back-off semantics from the sink runner
				result = Status.BACKOFF;
			}	

			if ( (event == null) && (rtxInfo.isStarted()) && (!rtxInfo.isFinished()) ) {	// The scenario: only parts of RTX unit are taken.
				String info_rollback = "Only parts of RTX unit are taken, thus we make the TRANSACTION roolback. [Just dance, no affects]. FROM: Ning Guo.";
				logger.info(info_rollback);
				throw new FlumeException(info_rollback);
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


	class RTXInfo{
		
		private String threadID;
		private Date time;
		private List<String> receivers;
		private String sender;
		
		public boolean isStarted() {
			return threadID != null ? true : false;
		}
		
		public boolean isFinished() {
			return ( (threadID != null) 
						&& (time != null)
						&& (receivers != null) 
						&& !receivers.isEmpty() 
						&& (sender != null) ) ? true : false;
		}
		
		public boolean isClean() {
			if ((threadID != null) 
					|| (time != null)
					|| (receivers != null) 
					|| (sender != null)) {
				return false;
			}
			return true;
		}
		
		public void clearAll() {
			threadID = null;
			time = null;
			receivers = null;
			sender = null;
		}
		
		public String getThreadID() {
			return threadID;
		}
		public RTXInfo setThreadID(String ThreadID) {
			this.threadID = ThreadID;
			return this;
		}
		public List<String> getReceivers() {
			return receivers;
		}
		public RTXInfo setReceivers(List<String> receivers) {
			this.receivers = receivers;
			return this;
		}
		public String getSender() {
			return sender;
		}
		public RTXInfo setSender(String sender) {
			this.sender = sender;
			return this;
		}
		public Date getTime() {
			return time;
		}
		public RTXInfo setTime(Date time) {
			this.time = time;
			return this;
		}
		
	}
	
	// for test, just adapt for Python edition.(First Edition)
	class RTXInfoFirstEdition{
		private String date;
		private List<String> receivers;
		private String sender;
		private String time;
		
		public RTXInfoFirstEdition retrieveFromRTXInfo(RTXInfo rtxInfo){
			String dateFormatString = "yyyyMMdd";
			String timeFormatString = "HHmmss";
			SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString);
			SimpleDateFormat timeFormat = new SimpleDateFormat(timeFormatString);
			
			this.date = dateFormat.format(rtxInfo.getTime());
			this.time = timeFormat.format(rtxInfo.getTime());
			
			this.sender = rtxInfo.getSender();
			receivers = rtxInfo.getReceivers();
			
			// Make receivers single String. (First Edition)
//			String receiverString = Arrays.toString(rtxInfo.getReceivers().toArray());
//			receiverString = receiverString.substring(1, receiverString.length()-1);
//			
//			receivers = new ArrayList<String>();
//			receivers.add(receiverString);
			
			return this;
		}
		
	}
	
}
