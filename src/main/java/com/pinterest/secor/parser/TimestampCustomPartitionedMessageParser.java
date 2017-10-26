package com.pinterest.secor.parser;

import java.util.Date;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

/**
 * To support S3 partition such as .../dt=2017-10-26/custom=xyz
 * to use the parser, set 
 * secor.common.properties: message.partition.customfield.name=
 * secor.prod.backup.properties: secor.message.parser.class=com.pinterest.secor.parser.TimestampCustomPartitionedMessageParser
 * 
 * @author richardxin
 *
 */
public class TimestampCustomPartitionedMessageParser extends TimestampedMessageParser {
    private final boolean m_timestampRequired;
    

    public TimestampCustomPartitionedMessageParser(SecorConfig config) {
        super(config);
        m_timestampRequired = config.isMessageTimestampRequired();
    }

    @Override
    public long extractTimestampMillis(final Message message) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        if (jsonObject != null) {
            Object fieldValue = getJsonFieldValue(jsonObject);
            
            if (fieldValue != null) {
                return toMillis(Double.valueOf(fieldValue.toString()).longValue());
            }
        } else if (m_timestampRequired) {
            throw new RuntimeException("Missing timestamp field for message: " + message);
        }
        return 0;
    }
    
    public String extractCustomFieldValue(final Message message){
    	JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
    	if (jsonObject != null) {
            Object fieldValue = jsonObject.get(this.mConfig.getMessagePartitionCustomFieldName());
            if (fieldValue != null) {
                return fieldValue.toString();
            }
        } 
    	return "";
    }
    
    /*
    protected String[] generatePartitions(long timestampMillis, String customFieldValue)
            throws Exception {
        Date date = new Date(timestampMillis);
        String dt = mDtPrefix + mDtFormatter.format(date);
        
        if (!customFieldValue.isEmpty()){
        	String custom = this.mConfig.getMessagePartitionCustomFieldName() + "=" + customFieldValue;
        	return new String[]{dt, custom};
        } else {
        	return new String[]{dt};
        }
    }*/
    
    // currently only support one custom partition field, it could later easily expand to multiple fields by suporting comma delimited value 
    protected String[] generatePartitions(long timestampMillis, boolean usingHourly, boolean usingMinutely, String customFieldValue)
            throws Exception {
        Date date = new Date(timestampMillis);
        String dt = mDtPrefix + mDtFormatter.format(date);
        String hr = mHrPrefix + mHrFormatter.format(date);
        String min = mMinPrefix + mMinFormatter.format(date);
        String custom = this.mConfig.getMessagePartitionCustomFieldName() + "=" + customFieldValue;
        
        if (usingMinutely) {
        	if (customFieldValue.isEmpty()){
        		return new String[]{dt, hr, min};
        	} else {
        		return new String[]{dt, hr, min, custom};
        	}
        } else if (usingHourly) {
        	if (customFieldValue.isEmpty()){
        		return new String[]{dt, hr};
        	} else {
        		return new String[]{dt, hr, custom};
        	}
        } else {
        	if (customFieldValue.isEmpty()){
        		return new String[]{dt};
        	} else {
        		return new String[]{dt, custom};
        	}
        }
    }
    
    @Override
    public String[] extractPartitions(Message message) throws Exception {
        // Date constructor takes milliseconds since epoch.
        long timestampMillis = getTimestampMillis(message);
        return generatePartitions(timestampMillis, mUsingHourly, mUsingMinutely, extractCustomFieldValue(message));
    }
}
