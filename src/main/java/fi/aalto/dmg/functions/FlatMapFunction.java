package fi.aalto.dmg.functions;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 21/10/15.
 */
// flink
//public interface FlatMapFunction {
//    public void flatMap(String value, Iterable<Tuple2<String, Integer>> out);
//}

// spark
public interface FlatMapFunction<T, R> extends Serializable {
    Iterable<R> flatMap(T var1) throws Exception;
}