package org.keedio.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

	private static final Logger LOG = LoggerFactory
			.getLogger(Main.class);

	public static void main(String[] args) throws Exception {
		String propertiesFile = args[0];
		LOG.debug(propertiesFile);
		TopologyProperties topologyProperties = new TopologyProperties(propertiesFile);
		KafkaElasticSearchTopology topology = new KafkaElasticSearchTopology(topologyProperties);
		topology.runTopology();
	}
}
