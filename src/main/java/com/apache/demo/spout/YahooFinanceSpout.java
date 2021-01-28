package com.apache.demo.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;

public class YahooFinanceSpout extends BaseRichSpout {
private SpoutOutputCollector collector;
private SimpleDateFormat format=new SimpleDateFormat("MM/dd/yyyy HH.mm.ss");
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        // StockQuote quote= YahooFinance.get("MSFT").getQuote();
        // BigDecimal price=quote.getPrice();
        //  BigDecimal prevClose=quote.getPreviousClose();
        Timestamp ts=new Timestamp(System.currentTimeMillis());
        collector.emit(new Values("MSFT",123,1233,format.format(ts)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("company","price","timestamp","prev_close"));
    }
}
