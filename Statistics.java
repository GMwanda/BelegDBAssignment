import java.util.ArrayList;

public class Statistics {
    private ArrayList<Double> values;
    private int length;

    public Statistics(ArrayList<Double> values) {
        this.values = new ArrayList<>(values);
        this.length = this.values.size();
    }

    public double calculateMean() {
        double sum = 0;
        for (double value : values) {
            sum += value;
        }
        return sum / length;
    }

    public double calculateVariance() {
        double mean = calculateMean();
        double sumSquaredDifferences = 0;

        for (double value : values) {
            sumSquaredDifferences += Math.pow(value - mean, 2);
        }
        return sumSquaredDifferences / length;
    }


    public double calculateStandardDeviation() {
        return Math.sqrt(calculateVariance());
    }

    public double findMin() {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("List is Empty");
        }
        double minimum = values.get(0);
        for (double value : values) {
            if (value < minimum) {
                minimum = value;
            }
        }
        return minimum;
    }

    public double findMax() {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("List is Empty");
        }
        double maximum = values.get(0);
        ;
        for (double value : values) {
            if (value > maximum) {
                maximum = value;
            }
        }
        return maximum;
    }


}