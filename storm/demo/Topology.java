package storm.demo;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class Topology {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", (new SimpleKafkaSpout()).topic("hamlet"));
		builder.setBolt("sentiment", new SentimentAnalysisBolt())
			.shuffleGrouping("spout");
		builder.setBolt("words", (new WordSentimentBolt()).redis("127.0.0.1"))
			.shuffleGrouping("sentiment");
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_WORKERS, 1);
		conf.setDebug(true);
		
		if (null != args && 0 < args.length) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("kafka-storm-sentiment-topology", conf, builder.createTopology());
		}
	}
}
