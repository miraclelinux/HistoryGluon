package com.miraclelinux.historygluon;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class BasicStorageDriver implements StorageDriver {

    /* -----------------------------------------------------------------------
     * Protected constant
     * -------------------------------------------------------------------- */
    protected static final int COUNT_UNLIMITED = -1;

    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private Log m_log = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public BasicStorageDriver() {
        m_log = LogFactory.getLog(BasicStorageDriver.class); 
    }

    @Override
    public boolean init() {
        return true;
    }

    @Override
    public void close() {
    }

    @Override
    public HistoryDataSet getData(long itemId, int clock0, int clock1)
      throws HistoryDataSet.TooManyException {
        String startKey = makeKey(itemId, clock0, 0);
        String stopKey = makeKey(itemId, clock1, 0);
        return getDataSet(itemId, startKey, stopKey, COUNT_UNLIMITED);
    }

    @Override
    public HistoryData getDataWithMinimumClock(long itemId)
      throws HistoryDataSet.TooManyException {
        final int max_count = 1;
        String startKey = makeKey(itemId, 0, 0);
        String stopKey = makeKey(itemId+1, 0, 0);
        HistoryDataSet dataSet = getDataSet(itemId, startKey, stopKey, max_count);
        if (dataSet.isEmpty())
            return null;
        return dataSet.first();
    }

    @Override
    public HistoryData getDataWithTimestamp(long itemId, int clock,
                                            int ns, boolean searchNear)
      throws HistoryDataSet.TooManyException {
        HistoryDataSet dataSet = null;
        String key = makeKey(itemId, clock, ns);
        dataSet = getDataSet(itemId, key, key, 1);
        if (!dataSet.isEmpty())
            return dataSet.first();
        if (searchNear == false)
            return null;

        // search the near data
        // TODO: We should make this method more fast.
        // HBase doen't have inverse order scan, does it ?
        String key0 = makeKey(itemId, 0, 0);
        dataSet = getDataSet(itemId, key0, key, COUNT_UNLIMITED);
        if (dataSet.isEmpty())
            return null;
        return dataSet.descendingIterator().next();
    }

    @Override
    public Statistics getStatistics(long itemId, int clock0, int clock1)
      throws HistoryDataSet.TooManyException {
        // extract data set
        String startKey = makeKey(itemId, clock0, 0);
        String stopKey;
        if (clock1 != 0)
            stopKey = makeKey(itemId, clock1, 0);
        else
            stopKey = makeKey(itemId+1, 0, 0);
        HistoryDataSet dataSet = getDataSet(itemId, startKey, stopKey, COUNT_UNLIMITED);

        // calcurate values
        Statistics statistics = new Statistics(itemId, clock0, clock1);
        Iterator<HistoryData> it = dataSet.iterator();
        while (it.hasNext()) {
            HistoryData history = it.next();
            try {
                if (statistics.count == 0)
                    statistics.setData(history);
                else
                    statistics.addData(history);
            } catch (HistoryData.DataNotNumericException e) {
                m_log.warn("Not value type: " + history.toString() +
                           ", key: " + history.key);
            }
        }
        return statistics;
    }

    public int delete(long itemId, int thresClock) {
        int numDeleted = 0;
        String startKey = makeKey(itemId, 0, 0);
        String stopKey = makeKey(itemId, thresClock, 0);
        HistoryDataSet dataSet = null;
        try {
            dataSet = getDataSet(itemId, startKey, stopKey, COUNT_UNLIMITED);
        } catch (HistoryDataSet.TooManyException e) {
            // FIXME: try to delete with a certin number of items
            m_log.error("Entries are too many: Cannot delete items.");
            return 0;
        }

        Object arg = deleteRowsPreAction();
        if (arg == null)
            return 0;

        Iterator<HistoryData> it = dataSet.iterator();
        while (it.hasNext()) {
            HistoryData history = it.next();
            if (deleteRow(history, arg))
                numDeleted++;
        }
        return numDeleted;
    }

    /* -----------------------------------------------------------------------
     * Protected Methods
     * -------------------------------------------------------------------- */
    protected abstract HistoryDataSet
      getDataSet(long itemId, String startKey, String stopKey, int maxCount) 
      throws HistoryDataSet.TooManyException;

    protected abstract boolean deleteRow(HistoryData history, Object arg);

    protected Object deleteRowsPreAction() {
        return true;
    }

    protected String makeKey(long itemId, int clock, int ns) {
        return String.format("%016x%08x%08x", itemId, clock, ns);
    }

    protected void fillItemIdClockNsWithKey(String key, HistoryData history) {
        if (key.length() != 32)
            throw new InternalCheckException("Invalid key length: " + key.length());
        String itemIdStr = key.substring(0,16);
        String clockStr = key.substring(16,24);
        String nsStr = key.substring(24,32);

        history.itemId = Long.parseLong(itemIdStr, 16);
        history.clock = Integer.parseInt(clockStr, 16);
        history.ns = Integer.parseInt(nsStr, 16);
    }
}
