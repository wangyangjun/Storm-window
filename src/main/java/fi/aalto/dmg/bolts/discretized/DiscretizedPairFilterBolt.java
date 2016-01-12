package fi.aalto.dmg.bolts.discretized;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import fi.aalto.dmg.functions.FilterFunction;
import fi.aalto.dmg.util.BoltConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jun on 11/13/15.
 */

public class DiscretizedPairFilterBolt<K, V> extends DiscretizedBolt {
    private static final Logger logger = LoggerFactory.getLogger(DiscretizedMapBolt.class);
    private static final long serialVersionUID = -1278162603589917250L;

    private Map<Integer, List<Tuple2<K,V>>> slideDataMap;
    private FilterFunction<Tuple2<K, V>> fun;

    public DiscretizedPairFilterBolt(FilterFunction<Tuple2<K, V>> function, String preComponentId) {
        super(preComponentId);
        this.fun = function;
        slideDataMap = new HashMap<>(BUFFER_SLIDES_NUM);
        for(int i=0; i<BUFFER_SLIDES_NUM; ++i){
            slideDataMap.put(i, new ArrayList<Tuple2<K,V>>());
        }
    }

    @Override
    public void processTuple(Tuple tuple) {
        try{
            int slideId = tuple.getInteger(0);
            slideId = slideId%BUFFER_SLIDES_NUM;
            K key = (K) tuple.getValue(1);
            V value = (V) tuple.getValue(2);
            List<Tuple2<K,V>> filterList = slideDataMap.get(slideId);
            if(null == filterList){
                filterList = new ArrayList<>();

            }
            Tuple2<K,V> t = new Tuple2<>(key, value);
            if(fun.filter(t)){
                filterList.add(t);
            }
            slideDataMap.put(slideId, filterList);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void processSlide(BasicOutputCollector collector, int slideIndex) {
        List<Tuple2<K,V>> list = slideDataMap.get(slideIndex);
        for(Tuple2<K,V> t : list) {
            collector.emit(new Values(slideIndex, t._1(), t._2()));
        }
        // clear data
        list.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declareStream(Utils.DEFAULT_STREAM_ID,
                new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
