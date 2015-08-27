package com.zhyea.storm.wordcount.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * @ClassName: PrintBolt 
 * @Description: 结果打印Bolt
 * @Site: www.zhyea.com 
 * @author robin
 * @date 2015年8月26日 下午3:29:51
 */
public class PrintBolt extends BaseRichBolt {

	private static final long serialVersionUID = -7968705335430337770L;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
	}

	@Override
	public void execute(Tuple input) {
		String result = input.getString(0);
		if (null == result) {
			return;
		}
		System.out.println(result);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
