package fi.aalto.dmg;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import fi.aalto.dmg.bolts.PrintBolt;
import fi.aalto.dmg.bolts.discretized.DiscretizedPairReduceByKeyBolt;
import fi.aalto.dmg.bolts.windowed.WindowPairReduceByKeyBolt;
import fi.aalto.dmg.exceptions.DurationException;
import fi.aalto.dmg.functions.ReduceFunction;
import fi.aalto.dmg.spout.IntervalSpout;
import fi.aalto.dmg.util.BoltConstants;
import fi.aalto.dmg.util.TimeDurations;
import scala.Tuple2;
import storm.kafka.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for simple App.
 */
public class AppTest  {

    public static void main( String[] args ) throws DurationException {
        TimeDurations windowDuration = new TimeDurations(TimeUnit.SECONDS, 6);
        TimeDurations slideDuration = new TimeDurations(TimeUnit.SECONDS, 2);

        ReduceFunction<Integer> sum = new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer var1, Integer var2) throws Exception {
                return var1 + var2;
            }
        };

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new IntervalSpout(), 1);
        builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
        // window
        builder.setBolt("window",
                new WindowPairReduceByKeyBolt<>(sum, windowDuration, slideDuration),
                3)
                .localOrShuffleGrouping("split");
        builder.setBolt("count",
                new DiscretizedPairReduceByKeyBolt<>(sum, "window"),
                3)
                .fieldsGrouping("window", new Fields(BoltConstants.OutputKeyField))
                .allGrouping("window", BoltConstants.TICK_STREAM_ID);

        builder.setBolt("print", new PrintBolt<>(), 3).localOrShuffleGrouping("count");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("storm-window-test", conf, builder.createTopology());
    }


    public static class SplitSentence extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector){
            String[] words = tuple.getString(0).split(" ");
            for(String word: words) {
                if( null != word && !word.isEmpty()){
                    collector.emit(new Values(word.trim().toLowerCase(),1));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class CounterBolt extends BaseBasicBolt {
        private Map<String, Integer> counts = new HashMap<String, Integer>();


        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {

            for(Map.Entry<String, Integer> entry: counts.entrySet()){
                String word = entry.getKey();
                Integer count = entry.getValue();
                collector.emit(Utils.DEFAULT_STREAM_ID, new Values(word, count));
            }

            Tuple2<String, Integer> tuple2 = (Tuple2<String, Integer>) tuple.getValue(0);
            if(null != tuple2){
                Integer count = counts.get(tuple2._1());
                if (count == null)
                    count = tuple2._2();
                else
                    count += tuple2._2();
                counts.put(tuple2._1(), count);
                System.out.println(tuple2._1() + "\t" + String.valueOf(count));
            } else {
                System.out.println("Failed");
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream(Utils.DEFAULT_STREAM_ID, new Fields("word", "count"));
            declarer.declareStream("tick", new Fields());
        }

    }


}