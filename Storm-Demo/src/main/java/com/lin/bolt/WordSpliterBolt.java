package com.lin.bolt;


import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordSpliterBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -5653803832498574866L;

    public void execute(Tuple input, BasicOutputCollector collector) {
        String line = input.getString(0);
        String[] words = line.split("\\s+");
        for (String word : words) {
            word = word.trim();
            if (StringUtils.isNotBlank(word)) {
                word = word.toLowerCase();
                collector.emit(new Values(word));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));

    }

}
