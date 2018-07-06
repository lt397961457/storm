package com.yly.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/12/28.
 */
public class WordCountBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Map<String,Long> wordCountMap = new HashMap<String, Long>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = wordCountMap.get(word);
        if(count == null){
            count = 0L;
        }
        count ++;
        wordCountMap.put(word,count);
        System.out.println("单词计数" + word + "出现"+ count);
        outputCollector.emit(new Values(word,count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
