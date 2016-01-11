## Storm-window
It is easy to know that in storm we could use tick tuples to execute tasks at fixed intervals. More specifically, use following code in 
a bolt means that except receive records from previous component(spout or bolt), this bolt receives a tick record every 300s from an
internal spout with component id as `Constants.SYSTEM_COMPONENT_ID` and stream id as `Constants.SYSTEM_TICK_STREAM_ID`

    @Override
     public Map<String, Object> getComponentConfiguration() {
        // configure how often a tick tuple will be sent to our bolt
        Config conf = new Config();
          conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 300);
          return conf;
     }
     
     protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
    
But how to implement a slide/window functionality in storm? Such as a 300s window with slide interval of 30s. There are few points need
to consider carefully:  

1. Buffer data in current and future window       
1. Clean data in processed slides          
1. Determin in following bolt(after window), whether current window is done in all worker nodes of previous window bolt  
1. How about the case the processing results of later window in a node come eralier than processing results 
of former window in another node?      

![](https://github.com/wangyangjun/RealtimeStreamBenchmark/blob/master/images/20151112-1.jpg) 
