package com.miraclelinux.historygluon;

public class Statistics {
    public long itemId = -1;
    public int sec0 = -1;
    public int sec1 = -1;
    public long  count = 0;
    public double min = 0;
    public double max = 0;
    public double sum = 0;

    public String toString() {
        String minStr = "" + min;
        String maxStr = "" + max;

        String diffClockStr = "N/A";
        if (sec0 > 0 && sec1 > 0)
            diffClockStr = "" + (sec1 - sec0);

        String str = "ItemID: " + itemId +
                     ", sec: " + sec0 + " - " + sec1 +
                     " (" + diffClockStr + "), count: " + count +
                     ", min: " + minStr +
                     ", max: " + maxStr +
                     ", sum: " + sum;
        return str;
    }

    public Statistics(long iId, int clk0, int clk1) {
        itemId = iId;
        sec0 = clk0;
        sec1 = clk1;
    }

    public void setData(HistoryData history)
      throws HistoryData.DataNotNumericException {
        double data = history.getDataAsDouble();
        min = data;
        max = data;
        sum = data;
        count = 1;
    }

    public void addData(HistoryData history)
      throws HistoryData.DataNotNumericException {
        double data = history.getDataAsDouble();
        if (data < min)
            min = data;
        else if (data > max)
            max = data;
        sum += data;
        count++;
    }
}
