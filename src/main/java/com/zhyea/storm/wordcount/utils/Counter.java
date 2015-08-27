package com.zhyea.storm.wordcount.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * @ClassName: Counter 
 * @Description: 统计工具类
 * @Site: www.zhyea.com 
 * @author robin
 * @date 2015年8月26日 上午11:08:16
 */
public class Counter extends HashMap<String, Integer> {

	private static final long serialVersionUID = 1L;

	/**
	 * 向统计表中加入元素
	 * @param ele
	 * 		统计的单词
	 * @return
	 */
	public Integer put(String ele) {
		if (containsKey(ele)) {
			int count = get(ele);
			return this.put(ele, ++count);
		}
		return this.put(ele, 1);
	}

	/**
	 * 根据map的值进行排序
	 * @return
	 */
	public Map<String, Integer> sortByValue() {
		Set<Entry<String, Integer>> entrySet = this.entrySet();
		LinkedList<Entry<String, Integer>> entryList = new LinkedList<Entry<String, Integer>>(entrySet);
		Collections.sort(entryList, new Comparator<Entry<String, Integer>>() {
			@Override
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		for (Entry<String, Integer> e : entryList) {
			sortedMap.put(e.getKey(), e.getValue());
		}
		return sortedMap;
	}
}
