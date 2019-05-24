package apachstorm;

import java.util.Map;
import java.util.Random;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

//import storm tuple packages
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

//import Spout interface packages
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Emits a random integer and a timestamp value (offset by one day),
 * every 100 ms. The ts field can be used in tuple time based windowing.
 */
public class RandomTempSpout extends BaseRichSpout {
    //private static final Logger LOG = LoggerFactory.getLogger(RandomTempSpout.class);
    private SpoutOutputCollector collector;
    private Random rand;
    private long sensorID = 0;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value", "sendsorid"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        sensorID++;
        Object msgId = "MsgID" + sensorID;
        collector.emit(new Values(rand.nextInt(200), "sensorid :" + sensorID), msgId);
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("The temperautre is higher than 100");
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("Normal");
    }
}
