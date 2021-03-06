package org.keedio.storm;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hmsonline.storm.elasticsearch.StormElasticSearchConstants;
import backtype.storm.Config;

public class TopologyProperties {
	
	private String kafkaTopic;
	private String topologyName;
	private int localTimeExecution;
	private Config stormConfig;
	private String zookeeperHosts;
	private String stormExecutionMode;
	private boolean startFromBeginnig;
	public static final Logger log = LoggerFactory
			.getLogger(TopologyProperties.class);
	
	public TopologyProperties(String fileName){
		
		stormConfig = new Config();

		try {
			setProperties(fileName);
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}
	
	private Properties readPropertiesFile(String fileName) throws IOException {
		Properties properties = new Properties();
		FileInputStream in = new FileInputStream(fileName);
		properties.load(in);
		in.close();
		return properties;		
	}
	
	private void setProperties(String fileName) throws IOException {
		
		Properties properties = readPropertiesFile(fileName);
		topologyName = properties.getProperty("storm.topology.name","topologyName");
		localTimeExecution = Integer.parseInt(properties.getProperty("storm.local.execution.time","20000"));
		kafkaTopic = properties.getProperty("kafka.topic");
		startFromBeginnig = Boolean.valueOf(properties.getProperty("kafka.startsFromBeginning"));
		setStormConfig(properties);
	}

	public boolean getStartFromBeginnig() {
		return startFromBeginnig;
	}

	public void setStartFromBeginnig(boolean startFromBeginnig) {
		this.startFromBeginnig = startFromBeginnig;
	}

	private void setStormConfig(Properties properties)
	{
		stormExecutionMode = properties.getProperty("storm.execution.mode","local");
		int stormWorkersNumber = Integer.parseInt(properties.getProperty("storm.workers.number","2"));
		int maxTaskParallism = Integer.parseInt(properties.getProperty("storm.max.task.parallelism","2"));
		zookeeperHosts = properties.getProperty("zookeeper.hosts");
		int topologyBatchEmitMillis = Integer.parseInt(
				properties.getProperty("storm.topology.batch.interval.miliseconds","2000"));
		String nimbusHost = properties.getProperty("storm.nimbus.host","localhost");
		String nimbusPort = properties.getProperty("storm.nimbus.port","6627");
		
		// How often a batch can be emitted in a Trident topology.
		stormConfig.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, topologyBatchEmitMillis);
		stormConfig.setNumWorkers(stormWorkersNumber);
		stormConfig.setMaxTaskParallelism(maxTaskParallism);
		// Storm cluster specific properties
		stormConfig.put(Config.NIMBUS_HOST, nimbusHost);
		stormConfig.put(Config.NIMBUS_THRIFT_PORT, Integer.parseInt(nimbusPort));
		stormConfig.put(Config.STORM_ZOOKEEPER_PORT, parseZkPort(zookeeperHosts));
		stormConfig.put(Config.STORM_ZOOKEEPER_SERVERS, parseZkHosts(zookeeperHosts));
		// Elastic Search specific properties
		stormConfig.put(StormElasticSearchConstants.ES_HOST, properties.getProperty("elasticsearch.host", "localhost"));
		stormConfig.put(StormElasticSearchConstants.ES_PORT, Integer.parseInt(properties.getProperty("elasticsearch.port", "9300")));
		stormConfig.put(StormElasticSearchConstants.ES_CLUSTER_NAME, properties.getProperty("elasticsearch.cluster.name"));
		stormConfig.put("elasticsearch.index", properties.getProperty("elasticsearch.index"));
		stormConfig.put("elasticsearch.type", properties.getProperty("elasticsearch.type"));
		stormConfig.put("other.simulated", properties.getProperty("other.simulated"));
	}

	private static int parseZkPort(String zkNodes) 
	{
		String[] hostsAndPorts = zkNodes.split(",");
		return  Integer.parseInt(hostsAndPorts[0].split(":")[1]);
	}
	
	private static List<String> parseZkHosts(String zkNodes) {

		String[] hostsAndPorts = zkNodes.split(",");
		List<String> hosts = new ArrayList<String>(hostsAndPorts.length);

		for (int i = 0; i < hostsAndPorts.length; i++) {
			hosts.add(i, hostsAndPorts[i].split(":")[0]);
		}
		return hosts;
	}
	
	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public String getTopologyName() {
		return topologyName;
	}

	public int getLocalTimeExecution() {
		return localTimeExecution;
	}

	public Config getStormConfig() {
		return stormConfig;
	}
	
	public String getZookeeperHosts() {
		return zookeeperHosts;
	}
	
	public String getStormExecutionMode() {
		return stormExecutionMode;
	}
	
	

	
}
