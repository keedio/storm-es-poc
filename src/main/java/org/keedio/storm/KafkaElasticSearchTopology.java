package org.keedio.storm;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.keedio.storm.bolts.KafkaParserBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import com.hmsonline.storm.contrib.bolt.elasticsearch.ElasticSearchBolt;
import com.hmsonline.storm.contrib.bolt.elasticsearch.mapper.DefaultTupleMapper;
import com.hmsonline.storm.contrib.bolt.elasticsearch.mapper.TupleMapper;

public class KafkaElasticSearchTopology {

	private final TopologyProperties topologyProperties;

	public KafkaElasticSearchTopology(TopologyProperties topologyProperties) {
		this.topologyProperties = topologyProperties;
	}
	
	public void runTopology() throws AlreadyAliveException, InvalidTopologyException, InterruptedException {

		StormTopology stormTopology = buildTopology();
		String stormExecutionMode = topologyProperties.getStormExecutionMode();
	
		switch (stormExecutionMode){
			case "cluster":
				StormSubmitter.submitTopology(topologyProperties.getTopologyName(), topologyProperties.getStormConfig(), stormTopology);
				break;
			case "local":
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

		String topologyName = topologyProperties.getTopologyName();

		String kafkaTopic = topologyProperties.getKafkaTopic();

		SpoutConfig kafkaConfig = new SpoutConfig(kafkaBrokerHosts, kafkaTopic, "/storm/kafka/" + topologyName, kafkaTopic);
		
		
		kafkaConfig.forceFromStart = topologyProperties.getStartFromBeginnig();
		
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
