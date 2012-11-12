/* History Gluon
   Copyright (C) 2012 MIRACLE LINUX CORPORATION
 
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

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
    private boolean m_stopKeyMinus1 = false;

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
    public HistoryDataSet getData(long id, int sec0, int ns0, int sec1, int ns1)
      throws HistoryDataSet.TooManyException {
        String startKey = makeKey(id, sec0, ns0);
        String stopKey = makeStopKey(id, sec1, ns1);
        return getDataSet(id, startKey, stopKey, COUNT_UNLIMITED);
    }

    @Override
    public HistoryData getMinimumTime(long id)
      throws HistoryDataSet.TooManyException {
        final int max_count = 1;
        String startKey = makeKey(id, 0, 0);
        String stopKey = makeStopKey(id+1, 0, 0);
        HistoryDataSet dataSet = getDataSet(id, startKey, stopKey, max_count);
        if (dataSet.isEmpty())
            return null;
        return dataSet.first();
    }

    @Override
    public HistoryData queryData(long id, int sec, int ns, int queryType)
      throws HistoryDataSet.TooManyException {
        HistoryDataSet dataSet = null;
        String key0 = makeKey(id, sec, ns);
        String key1 = makeKey(id, sec, ns+1);
        dataSet = getDataSet(id, key0, key1, 1);
        if (!dataSet.isEmpty())
            return dataSet.first();
        if (queryType == QueryType.ONLY_MATCH)
            return null;

        // search the closest large data
        if (queryType == QueryType.GREATER_DATA) {
            key0 = key1;
            key1 = makeKey(id+1, 0, 0);
            dataSet = getDataSet(id, key0, key1, 1);
            if (dataSet.isEmpty())
                return null;
            return dataSet.first();
        }

        // search the closest small data
        // TODO: We should make this method more fast.
        // HBase doen't have inverse order scan, does it ?
        if (queryType == QueryType.LESS_DATA) {
            String keyS = makeKey(id, 0, 0);
            dataSet = getDataSet(id, keyS, key0, COUNT_UNLIMITED);
            if (dataSet.isEmpty())
                return null;
            return dataSet.descendingIterator().next();
        }

        String msg = "Unknown query type: " + queryType;
        throw new InternalCheckException(msg);
    }

    @Override
    public HistoryStream getAllDataStream() {
        return new HistoryStream();
    }


    @Override
    public Statistics getStatistics(long id, int sec0, int ns0, int sec1, int ns1)
      throws HistoryDataSet.TooManyException, HistoryData.DataNotNumericException {
        // extract data set
        String startKey = makeKey(id, sec0, ns0);
        String stopKey = makeStopKey(id, sec1, ns1);
        HistoryDataSet dataSet = getDataSet(id, startKey, stopKey,
                                            COUNT_UNLIMITED);

        // calcurate values
        Statistics statistics = new Statistics(id, sec0, sec1);
        Iterator<HistoryData> it = dataSet.iterator();
        while (it.hasNext()) {
            HistoryData history = it.next();
            if (statistics.count == 0)
                statistics.setData(history);
            else
                statistics.addData(history);
        }
        return statistics;
    }

    public long delete(long id, int sec, int ns, short way) {
        int numDeleted = 0;
        String startKey;
        String stopKey;
        if (way == DeleteType.EQUAL) {
            startKey = makeKey(id, sec, ns);
            stopKey = makeStopKey(id, sec, ns+1);
        } else if (way == DeleteType.EQUAL_OR_LESS) {
            startKey = makeKey(id, 0, 0);
            stopKey = makeStopKey(id, sec, ns+1);
        } else if (way == DeleteType.LESS) {
            startKey = makeKey(id, 0, 0);
            stopKey = makeStopKey(id, sec, ns);
        } else if (way == DeleteType.EQUAL_OR_GREATER) {
            startKey = makeKey(id, sec, ns);
            stopKey = makeStopKey(id+1, 0, 0);
        } else if (way == DeleteType.GREATER) {
            startKey = makeKey(id, sec, ns+1);
            stopKey = makeStopKey(id+1, 0, 0);
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
            sec += (ns / 1000000000);
            ns = ns % 1000000000;
        }
        return String.format("%016x%08x%08x", id, sec, ns);
    }

    protected String makeStopKey(long id, int sec, int ns) {
        if (m_stopKeyMinus1) {
            if (ns == 0) {
                if (sec == 0) {
                    // We cannot do anything...
                } else {
                    sec--;
                    ns = 999999999;
                }
            } else
                ns--;
        }
        return makeKey(id, sec, ns);
    }

    protected void setStopKeyMinus1(boolean minus1) {
        m_stopKeyMinus1 = minus1;
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
