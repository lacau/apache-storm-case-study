package br.com.storm.study;

import br.com.storm.study.bolt.CreditCardPersistBolt;
import br.com.storm.study.bolt.CreditCardSaleBolt;
import br.com.storm.study.bolt.CreditCardSaveTextFileBolt;
import br.com.storm.study.bolt.CreditCardVerifyBinBolt;
import br.com.storm.study.spout.CreditCardSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by lacau on 27/04/16.
 */
public class Main {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("creditCardSpout", new CreditCardSpout(), 10);
        builder.setBolt("creditCardVerifyBinBolt", new CreditCardVerifyBinBolt(), 4).shuffleGrouping("creditCardSpout");
        builder.setBolt("creditCardSaleBolt", new CreditCardSaleBolt(), 4).shuffleGrouping("creditCardVerifyBinBolt");
        builder.setBolt("creditCardPersistBolt", new CreditCardPersistBolt(), 4).shuffleGrouping("creditCardSaleBolt");
        builder.setBolt("creditCardSaveTextFileBolt", new CreditCardSaveTextFileBolt(), 4).shuffleGrouping("creditCardPersistBolt");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("creditCardSpout", conf, builder.createTopology());
        Utils.sleep(2000);
        cluster.killTopology("creditCardSpout");
        cluster.shutdown();
    }
}