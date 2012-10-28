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
    public HistoryDataSet getData(long id, int sec0, int sec1)
      throws HistoryDataSet.TooManyException {
        String startKey = makeKey(id, sec0, 0);
        String stopKey = makeKey(id, sec1, 0);
        return getDataSet(id, startKey, stopKey, COUNT_UNLIMITED);
    }

    @Override
    public HistoryData getDataWithMinimumClock(long id)
      throws HistoryDataSet.TooManyException {
        final int max_count = 1;
        String startKey = makeKey(id, 0, 0);
        String stopKey = makeKey(id+1, 0, 0);
        HistoryDataSet dataSet = getDataSet(id, startKey, stopKey, max_count);
        if (dataSet.isEmpty())
            return null;
        return dataSet.first();
    }

    @Override
    public HistoryData getDataWithTimestamp(long id, int sec,
                                            int ns, boolean searchNear)
      throws HistoryDataSet.TooManyException {
        HistoryDataSet dataSet = null;
        String key = makeKey(id, sec, ns);
        dataSet = getDataSet(id, key, key, 1);
        if (!dataSet.isEmpty())
            return dataSet.first();
        if (searchNear == false)
            return null;

        // search the near data
        // TODO: We should make this method more fast.
        // HBase doen't have inverse order scan, does it ?
        String key0 = makeKey(id, 0, 0);
        dataSet = getDataSet(id, key0, key, COUNT_UNLIMITED);
        if (dataSet.isEmpty())
            return null;
        return dataSet.descendingIterator().next();
    }

    @Override
    public Statistics getStatistics(long id, int sec0, int sec1)
      throws HistoryDataSet.TooManyException {
        // extract data set
        String startKey = makeKey(id, sec0, 0);
        String stopKey;
        if (sec1 != 0)
            stopKey = makeKey(id, sec1, 0);
        else
            stopKey = makeKey(id+1, 0, 0);
        HistoryDataSet dataSet = getDataSet(id, startKey, stopKey, COUNT_UNLIMITED);

        // calcurate values
        Statistics statistics = new Statistics(id, sec0, sec1);
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

    public long delete(long id, int sec, int ns, short way) {
        int numDeleted = 0;
        String startKey;
        String stopKey;
        if (way == DeleteType.ONLY_MATCH) {
            startKey = makeKey(id, sec, ns);
            stopKey = startKey;
        } else if (way == DeleteType.EQUAL_OR_LESS) {
            startKey = makeKey(id, 0, 0);
            stopKey = makeKey(id, sec, ns+1);
        } else if (way == DeleteType.LESS) {
            startKey = makeKey(id, 0, 0);
            stopKey = makeKey(id, sec, ns);
        } else if (way == DeleteType.EQUAL_OR_GREATER) {
            startKey = makeKey(id, sec, ns);
            stopKey = makeKey(id+1, 0, 0);
        } else if (way == DeleteType.GREATER) {
            startKey = makeKey(id, sec, ns+1);
            stopKey = makeKey(id+1, 0, 0);
        } else
            throw new InternalCheckException("Unknown way type: " + way);

        HistoryDataSet dataSet = null;
        try {
            dataSet = getDataSet(id, startKey, stopKey, COUNT_UNLIMITED);
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

    /**
     *
     * @return The HistoryData instances that match the specified condition.
     *         Even when there is no matched items, empty HistoryDataSet is returned.
     *         When an error occured, null is returned.
     */
    protected abstract HistoryDataSet
      getDataSet(long id, String startKey, String stopKey, int maxCount) 
      throws HistoryDataSet.TooManyException;

    protected abstract boolean deleteRow(HistoryData history, Object arg);

    protected Object deleteRowsPreAction() {
        return true;
    }

    protected String makeKey(long id, int sec, int ns) {
        if (ns > 1000000000) {
            ns = ns % 1000000000;
            sec += (ns / 1000000000);
        }
        return String.format("%016x%08x%08x", id, sec, ns);
    }

    protected void fillItemIdClockNsWithKey(String key, HistoryData history) {
        if (key.length() != 32)
            throw new InternalCheckException("Invalid key length: " + key.length());
        String idStr = key.substring(0,16);
        String secStr = key.substring(16,24);
        String nsStr = key.substring(24,32);

        history.id = Utils.parseHexLong(idStr);
        history.sec = Utils.parseHexInt(secStr);
        history.ns = Utils.parseHexInt(nsStr);
    }
}
