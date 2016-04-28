package br.com.storm.study.bolt;

import java.util.Map;

import br.com.storm.study.entity.CreditCard;
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

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        CreditCard creditCard = (CreditCard) input.getValue(0);
        // Validate BIN
        creditCard.setBin("441111");
        collector.emit(input, new Values(creditCard));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("creditCard"));
    }
}