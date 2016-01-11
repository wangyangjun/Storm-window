package fi.aalto.dmg.functions;

import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by jun on 20/12/15.
 */
public interface FlatMapPairFunction<T, K, V> extends Serializable {
    Iterable<Tuple2<K,V>> flatMapToPair(T var1) throws Exception;
}