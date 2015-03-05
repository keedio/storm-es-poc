package org.buildoop.storm;

public class Main {


	public static void main(String[] args) throws Exception {
		String propertiesFile = args[0];
		System.out.println(propertiesFile);
		TopologyProperties topologyProperties = new TopologyProperties(propertiesFile);
		KafkaElasticSearchTopology topology = new KafkaElasticSearchTopology(topologyProperties);
		topology.runTopology();
	}
}
