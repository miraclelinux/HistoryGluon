package com.miraclelinux.historygluon;

import java.util.TreeSet;
import java.util.Comparator;

public class HistoryDataSet extends TreeSet<HistoryData> {

    // -----------------------------------------------------------------------
    // Public inner class
    // -----------------------------------------------------------------------
    public static class TooManyException extends Exception {
    }

    public static class EmptyException extends Exception {
    }

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public HistoryDataSet() {
    }

    public HistoryDataSet(Comparator comparator) {
        super(comparator);
    }
}
