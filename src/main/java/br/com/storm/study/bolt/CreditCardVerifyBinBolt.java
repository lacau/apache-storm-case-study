package br.com.storm.study.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import br.com.storm.study.entity.CreditCard;
import br.com.storm.study.spout.CreditCardSpout;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by lacau on 28/04/16.
 */
public class CreditCardVerifyBinBolt extends BaseRichBolt {

    private OutputCollector collector;

    public CreditCardVerifyBinBolt() {
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        CreditCardSpout.incrementCount();
        CreditCard creditCard = (CreditCard) input.getValue(0);
        BufferedWriter writer = null;
        try(FileWriter fw = new FileWriter("log.txt", true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw)) {
            out.println(toString() + " - " + creditCard.toString());
        } catch(IOException e) {
            e.printStackTrace();
        }
        // Validate BIN
        creditCard.setBin("441111");
        collector.emit(input, new Values(creditCard));

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("creditCard"));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CreditCardVerifyBinBolt{}");
        return sb.toString();
    }
}