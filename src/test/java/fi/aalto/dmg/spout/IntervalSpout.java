package fi.aalto.dmg.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * emit a record every 500ms
 * Created by jun on 11/01/16.
 */
public class IntervalSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private ArrayList<String> _sentences;
    private Iterator<String> _iterator;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _sentences = new ArrayList<>();
        _sentences.add("hello world");
        _sentences.add("hello jun");
        _sentences.add("jun boy");
        _sentences.add("good boy");
        _sentences.add("hi jason");
        _sentences.add("jack world");
        _sentences.add("hi jack");
        _sentences.add("beautiful world");
        _sentences.add("beautiful girl");
        _sentences.add("boy girl");

        _iterator = _sentences.iterator();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        if(_iterator.hasNext())
            _collector.emit(new Values(_iterator.next()));
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
