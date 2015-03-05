package org.buildoop.storm;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import org.buildoop.storm.bolts.KafkaParserBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import com.hmsonline.storm.contrib.bolt.elasticsearch.ElasticSearchBolt;
import com.hmsonline.storm.contrib.bolt.elasticsearch.mapper.DefaultTupleMapper;
import com.hmsonline.storm.contrib.bolt.elasticsearch.mapper.TupleMapper;

public class KafkaElasticSearchTopology {
	public static final Logger LOG = LoggerFactory
			.getLogger(KafkaElasticSearchTopology.class);

	private final TopologyProperties topologyProperties;

	public KafkaElasticSearchTopology(TopologyProperties topologyProperties) {
		this.topologyProperties = topologyProperties;
	}
	
	public void runTopology() throws Exception{

		StormTopology stormTopology = buildTopology();
		String stormExecutionMode = topologyProperties.getStormExecutionMode();
	
		switch (stormExecutionMode){
			case ("cluster"):
				StormSubmitter.submitTopology(topologyProperties.getTopologyName(), topologyProperties.getStormConfig(), stormTopology);
				break;
			case ("local"):
			default:
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topologyProperties.getTopologyName(), topologyProperties.getStormConfig(), stormTopology);
				Thread.sleep(topologyProperties.getLocalTimeExecution());
				cluster.killTopology(topologyProperties.getTopologyName());
				cluster.shutdown();
				System.exit(0);
		}	
	}
	
	private StormTopology buildTopology()
	{
		BrokerHosts kafkaBrokerHosts = new ZkHosts(topologyProperties.getZookeeperHosts());
		SpoutConfig kafkaConfig = new SpoutConfig(kafkaBrokerHosts, topologyProperties.getKafkaTopic(), "",	"storm");
		
		//TODO : configurable!!
		kafkaConfig.forceFromStart = true;
		
		TopologyBuilder builder = new TopologyBuilder();
		// Elastic search bolt
		TupleMapper tupleMapper = new DefaultTupleMapper();
		ElasticSearchBolt elasticSearchBolt = new ElasticSearchBolt(tupleMapper);
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 1);
		builder.setBolt("ParserBolt", new KafkaParserBolt(), 1).shuffleGrouping("KafkaSpout");
		builder.setBolt("ElasticSearchBolt", elasticSearchBolt, 1)
		.fieldsGrouping("ParserBolt", new Fields("id", "index", "type", "document"));
		return builder.createTopology();
	}
}
