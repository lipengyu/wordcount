package com.zhyea.storm.wordcount.bolt;

import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.zhyea.storm.wordcount.utils.Counter;

/**
 * @ClassName: WordCountBolt 
 * @Description: 单词统计，获取单词总数的TOP N，并发射出去
 * @Site: www.zhyea.com 
 * @author robin
 * @date 2015年8月26日 上午10:46:49
 */
public class WordCountBolt extends BaseRichBolt {

	private static final long serialVersionUID = 6385501338231964593L;

	private OutputCollector collector;
	//单词统计工具
	private Counter counter;

	/**
	 * Bolt初始化方法
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.counter = new Counter();
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getString(0);
		counter.put(word);
		Map<String, Integer> map = counter.sortByValue();
		StringBuilder builder = new StringBuilder();
		int count = 0;
		for (Entry<String, Integer> e : map.entrySet()) {
			builder.append(e.getKey()).append("-").append(e.getValue()).append(";");
			if (++count > 300) {
				break;
			}
		}
		collector.emit(new Values(builder.toString()));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
