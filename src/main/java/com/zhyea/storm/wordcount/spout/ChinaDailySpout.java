package com.zhyea.storm.wordcount.spout;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * @ClassName: ChinaDailySpout 
 * @Description: 抓取china daily网页内容，并发送出去
 * @site www.zhyea.com
 * @author robin
 * @date 2015年8月24日 下午10:52:31
 */
public class ChinaDailySpout extends BaseRichSpout {

	private static final long serialVersionUID = 9058512571341016091L;

	SpoutOutputCollector collector;

	/**
	 * 完成一些spout的初始化操作
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * 负责消息接入
	 */
	@Override
	public void nextTuple() {
		Utils.sleep(3000);//3秒钟发送一次数据
		ArrayList<String> allLinks = new ArrayList<String>();
		LinkedList<String> waitingLinks = new LinkedList<String>();
		String url = "http://www.chinadaily.com.cn/";
		waitingLinks.add(url);
		allLinks.add(url);
		while (!waitingLinks.isEmpty()) {
			String content = fetch(waitingLinks, allLinks);
			if (null == content || "".equals(content)) {
				continue;
			}
			collector.emit(new Values(content));
		}
	}

	/**
	 * 字段声明
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	/**
	 * 抓取网页内容，获得网页链接
	 * @param waittingLinks
	 * 			待处理链接
	 * @param allLinks
	 * 			所有链接，用于去重处理
	 * @return
	 */
	private String fetch(LinkedList<String> waittingLinks, List<String> allLinks) {
		Element body = null;
		String url = waittingLinks.removeFirst();
		if (null == url) {
			return "";
		}
		try {
			Document doc = Jsoup.connect(url).ignoreContentType(true).timeout(60000).get();
			body = doc.body();
		} catch (Exception e) {
			return "";
		}
		//获取页面上所有的超链接
		Elements links = body.select("a[href]");
		for (Element link : links) {
			String absLink = link.absUrl("href");//将相对路径转换为绝对路径
			if (!absLink.contains("www.chinadaily.com.cn")) {
				continue;//只要china daily站内连接
			}
			if (absLink.contains("#")) {
				continue;//避免同一网页重复获取
			}
			if (allLinks.contains(absLink)) {
				continue;//做去重处理
			}
			waittingLinks.add(absLink);
			allLinks.add(absLink);
		}

		return body.text();
	}

}
