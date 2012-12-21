package com.miraclelinux.historygluon;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NullDriver extends BasicStorageDriver {

    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private Log m_log = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public NullDriver(String[] args) {
        super(args);
        m_log = LogFactory.getLog(NullDriver.class); 
    }

    @Override
    public boolean init() {
        return true;
    }

    @Override
    public StorageDriver createInstance() {
        return this;
    }

    @Override
    public String getName() {
        return "Null";
    }

    @Override
    public void setDatabase(String dbName) {
    }

    @Override
    public int addData(HistoryData history) {
        return ErrorCode.SUCCESS;
    }

    @Override
    public boolean deleteDB() {
        return true;
    }

    /* -----------------------------------------------------------------------
     * Protected Methods
     * -------------------------------------------------------------------- */
    @Override
    protected HistoryDataSet
      getDataSet(String startKey, String stopKey, long maxCount) {
        HistoryDataSet dataSet = new HistoryDataSet();
        return dataSet;
    }

    @Override
    protected boolean deleteRow(HistoryData history, Object arg) {
        return true;
    }

}
