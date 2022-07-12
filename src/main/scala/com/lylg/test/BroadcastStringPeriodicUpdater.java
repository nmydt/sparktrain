package com.lylg.test;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.HashSet;

public class BroadcastStringPeriodicUpdater {
    private static final int PERIOD = 60 * 1000;
    private static volatile BroadcastStringPeriodicUpdater instance;

    private Broadcast<Long> broadcast;
    private final HashSet<String> set;

    private BroadcastStringPeriodicUpdater() {
        set = new HashSet<>();
    }

    public static BroadcastStringPeriodicUpdater getInstance() {
        if (instance == null) {
            synchronized (BroadcastStringPeriodicUpdater.class) {
                if (instance == null) {
                    instance = new BroadcastStringPeriodicUpdater();
                }
            }
        }
        return instance;
    }

    public static void main(String[] args) {
//        String broadcastValue = BroadcastStringPeriodicUpdater.getInstance().updateAndGet(rdd.context());
    }

    public Long updateAndGet(SparkContext sc, String user_id) {
        long now = System.currentTimeMillis();
//        long offset = now - lastUpdate;
        if (set.contains(user_id)) {
            if (broadcast != null) {
                broadcast.unpersist();
            }

            Long value = fetchBroadcastValue();
            broadcast = JavaSparkContext.fromSparkContext(sc).broadcast(value);
        }
        return broadcast.getValue();
    }

    private Long fetchBroadcastValue() {
        return 0L;
    }

}