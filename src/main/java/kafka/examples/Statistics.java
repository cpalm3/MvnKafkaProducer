package kafka.examples;

import java.util.*;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;


public class Statistics {
    final private List<Metrics> stats;

    public Statistics(int size) {
        this.stats = new ArrayList<>(size);
    }
    public Statistics(){
    	this.stats = null;
    }

    public void add(Metrics metrics) {
        this.stats.add(metrics);
    }

    
  
    	public void showDrop(Meter rateMeter,
                Histogram histogram
               ) {
   System.out.format("throughput %.2fmessages/s; latency: average - %.2fms, medium - %.2fms, 99%% - %.2fms; \n",
        //   sizeRate.getOneMinuteRate() / 1000,
           rateMeter.getOneMinuteRate(),
           histogram.getSnapshot().getMean(),
           histogram.getSnapshot().getMedian(),
           histogram.getSnapshot().get99thPercentile()
         //  errorCounter.getCount()
           );
}
    
    public void show() {
        Collections.sort(stats, new Comparator<Metrics>() {
            @Override
            public int compare(Metrics m1, Metrics m2) {
                return Double.compare(m1.getDuration(), m2.getDuration());
            }
        });

        double totalSize=0.0, totalTime=0.0, totalCounts = 0.0;
        int totalErrors = 0;
        for(Metrics stat : stats) {
            totalTime += stat.getDuration();
            totalSize += stat.getSize();
            totalCounts += stat.getCounts();
            totalErrors += stat.getStatus();
        }

//        System.out.print("throughput: "+ totalSize/totalTime/1024.0 +
//                "KB/s; latency: average - " + totalTime/stats.size() +
//                "ms, medium - " + stats.get((stats.size()-1)/2).getDuration() +
//                "ms, 99% - " + stats.get((int)((stats.size()-1.0)*0.99)).getDuration() +
//                "ms"
//        );
        System.out.format("throughput: %.2fKB/s, %.2fmessages/s; latency: average - %.2fms, medium - %.2fms, 99%% - %.2fms; errors: %d\n",
                totalSize/totalTime/1.024,
                totalCounts*1000.0/totalTime, //All batches * 1000 to adjust to secondes when diving by milliseconds to get seconds
                totalTime/stats.size(),
                stats.get((stats.size()-1)/2).getDuration(),
                stats.get((int)((stats.size()-1.0)*0.99 + 0.5)).getDuration(),
                totalErrors
                );
    }
}
