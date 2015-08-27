package com.zhyea.storm.wordcount.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @ClassName: WordNormalizerBolt 
 * @Description: 消息标准化处理
 * @Site: www.zhyea.com 
 * @author robin
 * @date 2015年8月26日 上午10:46:59
 */
public class WordNormalizerBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1244672469187107577L;

	private OutputCollector collector;

	private static String COMMON = "the,in,to,of,china,for,and,a,on,|,chinese,with,from,daily,us,world,at,is,this,by,not,top,be,or,are,more,as,china's,beijing,10, ,new,page,site,content,multimedia-194,-,that,about,but,most,it,comments,news,home,business,next,all,has,i,will";

	/**
	 * Bolt的初始化方法
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * 订阅tuple的处理逻辑
	 */
	@Override
	public void execute(Tuple input) {
		String content = input.getString(0);
		String[] words = content.split(" ");
		for (String w : words) {
			if (COMMON.contains(w.toLowerCase())) {
				continue;
			}
			collector.emit(new Values(w.toLowerCase()));
		}
	}

	/**
	 * 字段声明
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
