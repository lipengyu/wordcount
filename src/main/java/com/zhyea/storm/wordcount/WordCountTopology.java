package com.zhyea.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.zhyea.storm.wordcount.bolt.PrintBolt;
import com.zhyea.storm.wordcount.bolt.WordCountBolt;
import com.zhyea.storm.wordcount.bolt.WordNormalizerBolt;
import com.zhyea.storm.wordcount.spout.ChinaDailySpout;

/**
 * @ClassName: WordCountTopology 
 * @Description: 启动主类，构建拓扑
 * @Site: www.zhyea.com 
 * @author robin
 * @date 2015年8月26日 下午3:35:57
 */
public class WordCountTopology {

	private static TopologyBuilder builder = new TopologyBuilder();

	public static void main(String[] args) {
		Config config = new Config();
		//设置Spout，任务数为3
		builder.setSpout("ChinaDaily", new ChinaDailySpout(), 3);
		//添加WordNormalizerBolt，任务数为2，使用了随机分组
		builder.setBolt("WordNormalizer", new WordNormalizerBolt(), 2).shuffleGrouping("ChinaDaily");
		//添加WordCountBolt，任务数为2，使用了字段分组
		builder.setBolt("WordCount", new WordCountBolt(), 2).fieldsGrouping("WordNormalizer", new Fields("word"));
		//添加PrintBolt，任务数为1，使用了随随机分组
		builder.setBolt("Print", new PrintBolt(), 1).shuffleGrouping("WordCount");

		//使用调试模式
		config.setDebug(false);

		//使用本地集群模式提交Topology
		config.setMaxTaskParallelism(1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("WordCount", config, builder.createTopology());
	}
}
