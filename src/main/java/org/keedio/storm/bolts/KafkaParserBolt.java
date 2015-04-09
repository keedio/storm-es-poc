package org.keedio.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;
import java.util.Iterator;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.util.UUID;
import static backtype.storm.utils.Utils.tuple;

@SuppressWarnings("serial")
public class KafkaParserBolt implements IBasicBolt {

	private String index;
	private String type;
	private boolean simulated = true;
	private static final Logger LOG = LoggerFactory
			.getLogger(KafkaParserBolt.class);

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		index = (String) stormConf.get("elasticsearch.index");
		type = (String) stormConf.get("elasticsearch.type");
		simulated = "true".equals((String)stormConf.get("other.simulated"))?true:false;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String kafkaEvent = new String(input.getBinary(0));


		if (kafkaEvent.length()>0)
		{

			JSONObject objAux = new JSONObject();    		
			JSONParser parser = new JSONParser();
						
			try {
				Object obj = parser.parse(kafkaEvent);

				JSONObject jsonObject = (JSONObject) obj;
				String message = (String) jsonObject.get("message");
				JSONObject extraData = (JSONObject) jsonObject.get("extraData");
				Iterator iter = extraData.entrySet().iterator();
				
				while(iter.hasNext()){
				      Map.Entry entry = (Map.Entry)iter.next();
				      objAux.put((String)entry.getKey(), entry.getValue());
				}
				
				objAux.put("message",message);
				
				
				LOG.debug(message);

				int inicio = message.indexOf("keedio.datagenerator: ")+"keedio.datagenerator: ".length();

				objAux.put("timestamp",this.transformDate(message.substring(inicio, inicio + 23), "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"));

				collector.emit(tuple(String.valueOf(UUID.randomUUID()),index, (String)objAux.get(type), objAux.toString()));
			} catch (org.json.simple.parser.ParseException e) {
				LOG.error("Error al parsear mensaje: " + kafkaEvent);		
			} catch (ParseException e) {
				LOG.error("Error al formatear la fecha. Revisar formato de mensaje: " + kafkaEvent);
			} catch (Exception e) {
				LOG.error("Formato de mensaje inesperado: " + kafkaEvent);
			}

		}




	}


	private String transformDate(String date, String originPtt, String finalPtt) throws ParseException{

		SimpleDateFormat sdf1 = new SimpleDateFormat(originPtt);
		Date date1 = sdf1.parse(date);
		date1.setYear(new Date().getYear());
		if (simulated) 
			date1=new Date();

		SimpleDateFormat sdf2 = new SimpleDateFormat(finalPtt);

		return sdf2.format(date1);


	}

	@Override
	public void cleanup() {
		// Nothing to do 
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "index", "type", "document"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}


}
