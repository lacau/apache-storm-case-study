package br.com.storm.study;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by lacau on 27/04/16.
 */
public class Main {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("001", new TestWordSpout(), 10);
        builder.setBolt("bolt001", new MyBolt(), 3).shuffleGrouping("001");
        builder.setBolt("bolt002", new MyBolt(), 2).shuffleGrouping("bolt001");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("001", conf, builder.createTopology());
        cluster.killTopology("001");
        cluster.shutdown();
    }
}
