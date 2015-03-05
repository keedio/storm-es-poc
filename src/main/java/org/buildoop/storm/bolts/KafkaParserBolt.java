package org.buildoop.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import static backtype.storm.utils.Utils.tuple;

@SuppressWarnings("serial")
public class KafkaParserBolt implements IBasicBolt {
	
	private String index;
	private String type;
	private int i = 1000;
	//private String type;

	@SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
    	index = (String) stormConf.get("elasticsearch.index");
    	type = (String) stormConf.get("elasticsearch.type");
    	//this.type = (String) stormConf.get("elasticsearch.type");
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
    	String kafkaEvent = new String(input.getBinary(0));
    	
    	
    	if (kafkaEvent.length()>0)
    	{
    		
    		JSONObject objAux = new JSONObject();    		
    		JSONParser parser = new JSONParser();
    		try {
    			Object obj = parser.parse(kafkaEvent);
    			
    			JSONObject jsonObject = (JSONObject) obj;
    			System.out.println(obj);
    			String message = (String) jsonObject.get("message");
    			JSONObject extraData = (JSONObject) jsonObject.get("extraData");
    			objAux.put("message",message);
    			objAux.put("ciid",extraData.get("ciid"));
    			objAux.put("item",extraData.get("item"));
    			objAux.put("hostname",extraData.get("hostname"));
    			objAux.put("delivery",extraData.get("delivery"));
    			objAux.put("timestamp",transformDate(message.substring(4, 16), "MMM dd hh:mm", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
    			objAux.put("vdc", extraData.get("vdc"));
    			
    		} catch (org.json.simple.parser.ParseException e) {
    			
    		}
        	
        	collector.emit(tuple("" + i++,index, (String)objAux.get("delivery"), objAux.toString()));
    	}
    	
    	
    	
		
    }

    
	private static String transformDate(String date, String originPtt, String finalPtt) {
		try {
		SimpleDateFormat sdf1 = new SimpleDateFormat(originPtt);
		Date date1 = sdf1.parse(date);
		date1.setYear(new Date().getYear());
		
		SimpleDateFormat sdf2 = new SimpleDateFormat(finalPtt);
		
		return sdf2.format(date1);
		} catch (Exception e) {
			return "";
		}
		
	}
    
    
	public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "index", "type", "document"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    

}
