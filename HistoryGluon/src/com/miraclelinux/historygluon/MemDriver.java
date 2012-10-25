package com.miraclelinux.historygluon;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MemDriver extends BasicStorageDriver {

    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private Log m_log = null;
    private ConcurrentHistoryDataSet m_dataSet = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public MemDriver() {
        m_log = LogFactory.getLog(MemDriver.class); 
    }

    @Override
    public boolean init() {
        m_dataSet = new ConcurrentHistoryDataSet();
        return true;
    }

    @Override
    public StorageDriver createInstance() {
        return this;
    }

    @Override
    public String getName() {
        return "Mem";
    }

    @Override
    public boolean addData(HistoryData history) {
        return m_dataSet.add(history);
    }

    @Override
    public boolean deleteDB() {
        // Nothing to do, because data is lost when HistoryGluon process terminates.
        return true;
    }

    /* -----------------------------------------------------------------------
     * Protected Methods
     * -------------------------------------------------------------------- */
    @Override
    protected HistoryDataSet
      getDataSet(long itemId, String startKey, String stopKey, int maxCount) {
        HistoryData historyForComp0 = new HistoryData();
        HistoryData historyForComp1 = new HistoryData();
        fillItemIdClockNsWithKey(startKey, historyForComp0);
        fillItemIdClockNsWithKey(stopKey, historyForComp1);
        return m_dataSet.createSubSet(historyForComp0, historyForComp1);
    }

    @Override
    protected boolean deleteRow(HistoryData history, Object arg) {
        return m_dataSet.remove(history);
    }
}
