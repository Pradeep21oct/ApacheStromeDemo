package com.apache.demo.topology;

import com.apache.demo.bolt.YahooFinanceBolt;
import com.apache.demo.spout.YahooFinanceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {
    public static void main(String[] args) {
        //Build topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Spout", new YahooFinanceSpout());
        builder.setBolt("Bolt", new YahooFinanceBolt()).shuffleGrouping("Bolt");
        StormTopology topology=builder.createTopology();

        //Configuration
        Config cfg=new Config();
        cfg.setDebug(true);
        cfg.put("fileTowrite","output.txt");
        //Submit the topology to cluster
        LocalCluster cluster=new LocalCluster();
        try {
            cluster.submitTopology("Stock tracker topology",cfg,topology);
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            cluster.shutdown();
        }
    }
}
