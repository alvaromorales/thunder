package storm.thunder.util;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

/**
 * From nathanmarz/storm-starter
 * @author nathanmarz
 */
public final class TupleHelpers {

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

}