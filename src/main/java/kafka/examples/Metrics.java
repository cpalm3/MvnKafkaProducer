package kafka.examples;


public class Metrics {
    final private double duration;
    final private double size;
    final private int status;
    final private int counts;

    public Metrics(double duration, double size, int status, int counts) {
        this.duration = duration;
        this.size = size;
        this.status = status;
        this.counts = counts;
    }

    public double getDuration() {
        return duration;
    }

    public double getSize() {
        return size;
    }

    public int getCounts() {
        return counts;
    }

    public int getStatus() {
        return status;
    }
}
