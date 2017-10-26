package com.pinterest.secor.parser;

import java.util.ArrayList;
import java.util.List;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;


/**
 * To support S3 partition such as .../dt=2017-10-26/custom=xyz
 * to use the parser, set 
 * secor.common.properties: message.partition.customfield.name=part1,part2,part3
 * secor.prod.backup.properties: secor.message.parser.class=com.pinterest.secor.parser.CustomPartitionedMessageParser
 * 
 * @author richardxin
 * 
 * Still Under Construction
 *
 */

public class CustomPartitionedMessageParser extends MessageParser implements Partitioner {

	public CustomPartitionedMessageParser(SecorConfig config) {
		super(config);
		// TODO Auto-generated constructor stub
	}
	
    public String extractCustomFieldValue(final Message message, final String fieldname){
    	JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
    	if (jsonObject != null) {
            Object fieldValue = jsonObject.get(fieldname);
            if (fieldValue != null) {
                return fieldValue.toString();
            }
        }
    	return "";
    }

	@Override
	public String[] getFinalizedUptoPartitions(List<Message> lastMessages, List<Message> committedMessages)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getPreviousPartitions(String[] partition) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] extractPartitions(Message payload) throws Exception {
		return generatePartitions(payload);
	}
	
	protected String[] generatePartitions(final Message message){
		JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
		String strNames = this.mConfig.getMessagePartitionCustomFieldName();
		String[] names = strNames.split(",");
		
		List<String> list = new ArrayList<>();
		for (String n : names){
        	Object value = jsonObject.get(n);
        	if (value != null){
        		list.add(n + "=" + value.toString());
        	} else {
        		list.add(n + "=");
        	}
        }
		
		return list.toArray(new String[list.size()]);
	}
	

}
