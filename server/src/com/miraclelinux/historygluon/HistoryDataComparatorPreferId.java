package com.miraclelinux.historygluon;

import java.util.Comparator;

public class HistoryDataComparatorPreferId implements Comparator<HistoryData>
{
    @Override
    public int compare(HistoryData h0, HistoryData h1) {
        return HistoryData.comparePreferId(h0, h1);
    }
}
