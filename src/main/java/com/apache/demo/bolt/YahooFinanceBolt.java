package com.apache.demo.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class YahooFinanceBolt extends BaseRichBolt {
    private PrintWriter printWriter;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
       outputFieldsDeclarer.declare(new Fields("company","price","timestamp","gain"));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        String fileName=map.get("fileTowrite").toString();
        try {
            this.printWriter=new PrintWriter(fileName,"UTF-8");
        } catch (FileNotFoundException |UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String company=tuple.getString(0);
        String ts=tuple.getStringByField("timestamp");
        Double price=tuple.getDoubleByField("price");
        Double prePrice=tuple.getDoubleByField("prev_close");
        Boolean gain=false;
        if(price>prePrice) gain=true;
        printWriter.write(company+", "+ts+", "+price+", "+gain);
    }
    @Override
    public void cleanup() {
        printWriter.close();
    }
}
