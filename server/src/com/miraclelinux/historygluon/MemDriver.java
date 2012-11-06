package com.miraclelinux.historygluon;

import java.util.HashMap;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MemDriver extends BasicStorageDriver {

    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private Log m_log = null;
    private String m_dbName = null;
    //private ConcurrentHistoryDataSet m_dataSetPreferTime = null;
    private ConcurrentHistoryDataSet m_dataSetPreferId = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public MemDriver() {
        m_log = LogFactory.getLog(MemDriver.class); 
    }

    @Override
    public boolean init() {
        //m_dataSetPreferTime = new ConcurrentHistoryDataSet();
        m_dataSetPreferId =
          new ConcurrentHistoryDataSet(new HistoryDataComparatorPreferId());
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
    public void setDatabase(String dbName) {
        m_dbName = dbName;
    }

    @Override
    public int addData(HistoryData history) {
        /*
        if (!dataSetPreferTime.add(history)
            return ErrorCode.ENTRY_EXISTS;
        */
        if (!m_dataSetPreferId.add(history)) {
            //m_dataSetPreferTime.delete(history);
            return ErrorCode.ENTRY_EXISTS;
        }
        return ErrorCode.SUCCESS;
    }

    @Override
    public boolean deleteDB() {
        // Nothing to do, because data is lost when the server terminates.
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
        return m_dataSetPreferId.createSubSet(historyForComp0, historyForComp1);
    }

    @Override
    protected boolean deleteRow(HistoryData history, Object arg) {
        //boolean ret0 = m_dataSetPreferTime.delete(history);
        boolean ret1 = m_dataSetPreferId.delete(history);
        //return ret0 && ret1;
        return ret1;
    }

}
