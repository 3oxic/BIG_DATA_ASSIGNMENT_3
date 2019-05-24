package apachstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;

/**
 * A sample topology that demonstrates the usage of {@link org.apache.storm.topology.IWindowedBolt}
 * to calculate sliding window sum.
 */
public  class CheckTemperatureBolt extends BaseRichBolt {
    OutputCollector _collector;
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
    
    @Override
    public void execute(Tuple tuple) {
        int temp = tuple.getInteger(0);
        int m = 100;
        if (temp > m) {
            _collector.ack(tuple);
        }
        else{
            _collector.fail(tuple);
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result"));
    }
}  
