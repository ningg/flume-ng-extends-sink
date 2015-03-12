package com.github.ningg.flume.sink.elasticsearch.serializer;

import static com.github.ningg.flume.sink.elasticsearch.serializer.RTXElasticSearchIndexRequestBuilderFactoryConstants.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;


/**
 * 
 * @author Ning Guo
 * @time 2015-03-11
 * @see org.apache.flume.sink.elasticsearch.AbstractElasticSearchIndexRequestBuilderFactory
 *
 */
public class RTXElasticSearchIndexRequestBuilderFactory implements ElasticSearchIndexRequestBuilderFactory{

	private static final Logger logger = LoggerFactory.getLogger(RTXElasticSearchIndexRequestBuilderFactory.class);
	
	private SimpleDateFormat simpleDateFormat;
	private String charset = DEFAULT_CHARSET;
	private Gson gson;
	private ElasticSearchEventSerializer serializer;
	
	public RTXElasticSearchIndexRequestBuilderFactory(){
	}
	
	@Override
	public void configure(Context context) {
		String datePat = context.getString(DATE_PAT, DEFAULT_DATE_PAT);
		simpleDateFormat = new SimpleDateFormat(datePat);
		charset = context.getString(CHARSET, DEFAULT_CHARSET);
		gson = new Gson();
		
	}

	@Override
	public void configure(ComponentConfiguration conf) {
	}


	@Override
	public IndexRequestBuilder createIndexRequest(Client client,
			String indexPrefix, String indexType, Event event)
			throws IOException {
		IndexRequestBuilder request = client.prepareIndex();
		RTXInfoFirstEdition rtxInfo = constructRTXInfoFromEvent(event,charset);
		Date timestamp = new Date(); 
		try {
			timestamp = getTimeStamp(rtxInfo);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		String indexName = getIndexName(indexPrefix, timestamp);
		prepareIndexRequest(request, indexName, indexType, timestamp, rtxInfo);
		return request;
	}


	private void prepareIndexRequest(IndexRequestBuilder request,
			String indexName, String indexType, Date timestamp, RTXInfoFirstEdition rtxInfo) throws IOException {
		XContentBuilder builder = jsonBuilder().startObject();
		appendBody(builder, rtxInfo, timestamp);
		request.setIndex(indexName)
			   .setType(indexType)
			   .setSource(builder);
	}

	private void appendBody(XContentBuilder builder, RTXInfoFirstEdition rtxInfo, Date timestamp) throws IOException {
		// TODO Auto-generated method stub
		builder.field("timestamp", timestamp);
		builder.field("sender", rtxInfo.getSender());
		builder.field("receivers", rtxInfo.getReceivers());
	}

	
	private String getIndexName(String indexPrefix, Date timestamp) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String indexSuffix = dateFormat.format(timestamp);
		return indexPrefix + "-" + indexSuffix;
	}

	private Date getTimeStamp(RTXInfoFirstEdition rtxInfo) throws ParseException {
		Date timestamp = simpleDateFormat.parse(rtxInfo.getDate()+rtxInfo.getTime());
		return timestamp;
	}

	private RTXInfoFirstEdition constructRTXInfoFromEvent(Event event,
			String charset) throws UnsupportedEncodingException {
		String body = new String(event.getBody(), charset);
		RTXInfoFirstEdition rtxInfo = gson.fromJson(body, RTXInfoFirstEdition.class);
		return rtxInfo;
	}


	/**
	 *  for test, just adapt for Python edition.(First Edition)
	 * @see com.github.ningg.flume.sink.RTXManyToOneKafkaSink inner class RTXInfoFirstEdition.
	 *
	 */
	
	class RTXInfoFirstEdition{
		private String date;
		private List<String> receivers;
		private String sender;
		private String time;
		
		public String getDate() {
			return date;
		}
		public void setDate(String date) {
			this.date = date;
		}
		public List<String> getReceivers() {
			return receivers;
		}
		public void setReceivers(List<String> receivers) {
			this.receivers = receivers;
		}
		public String getSender() {
			return sender;
		}
		public void setSender(String sender) {
			this.sender = sender;
		}
		public String getTime() {
			return time;
		}
		public void setTime(String time) {
			this.time = time;
		}
		
	}

}
