package br.com.storm.study.spout;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import br.com.storm.study.entity.CreditCard;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * Created by lacau on 28/04/16.
 */
public class CreditCardSpout extends BaseRichSpout {

    public static Queue<CreditCard> transactionQueue = new LinkedList<CreditCard>();

    private boolean isDistributed;

    private SpoutOutputCollector collector;

    public static long boltsCount = 0;

    public CreditCardSpout() {
        this(true);
    }

    public CreditCardSpout(boolean isDistributed) {
        this.isDistributed = isDistributed;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("creditCard"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        CreditCard transaction = transactionQueue.poll();
        if(transaction != null) {
            collector.emit(new Values(transaction));
        }
        Utils.sleep(10);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        }

        return null;
    }

    public static void addTransaction(CreditCard creditCard) {
        transactionQueue.add(creditCard);
    }

    public static synchronized void incrementCount() {
        boltsCount++;
    }
}