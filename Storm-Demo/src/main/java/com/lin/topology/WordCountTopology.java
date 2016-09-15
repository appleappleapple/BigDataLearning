package com.lin.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.lin.bolt.WordCounterBolt;
import com.lin.bolt.WordSpliterBolt;
import com.lin.spout.WordReaderSpout;

/**
 * 功能概要：
 * 
 * @author linbingwen
 * @since 2016年8月28日
 */
public class WordCountTopology {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReaderSpout());
		builder.setBolt("word-spilter", new WordSpliterBolt()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounterBolt()).shuffleGrouping("word-spilter");

		Config conf = new Config();

		conf.setDebug(true);

		if (args.length >0 && "cluster".equals(args[0])) {
			StormSubmitter.submitTopology("Cluster-Storm-Demo", conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Local-Storm-Demo", conf, builder.createTopology());
		}
	}
}
