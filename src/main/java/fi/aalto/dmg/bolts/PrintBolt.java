package fi.aalto.dmg.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yangjun.wang on 02/11/15.
 */
public class PrintBolt<T> extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(PrintBolt.class);
    private static final long serialVersionUID = -5688326702164269931L;

    private boolean windowed;

    public PrintBolt(){ }
    public PrintBolt(boolean windowed) { this.windowed=windowed; }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        logger.error(input.getValues().toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
