package com.lin.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WordCounterBolt extends BaseBasicBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(WordCounterBolt.class);
	
    private static final long serialVersionUID = 5683648523524179434L;
    private HashMap<String, Integer> counters = new HashMap<String, Integer>();
    private volatile boolean edit = false;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
        new Thread(new Runnable() {
            public void run() {
                while (true) {
                    if (edit) {
                        logger.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<WordCounter result>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                        for (Entry<String, Integer> entry : counters.entrySet()) {
                            logger.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<Word:{},Count:{}>>>>>>>>>>>>>>>>>>>>>>>>>>>>",entry.getKey(),entry.getValue());
                        }
                        edit = false;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String str = input.getString(0);
        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
        edit = true;
        logger.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<WordCounter to add string ={} >>>>>>>>>>>>>>>>>>>>>>>>>>>>",str);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
