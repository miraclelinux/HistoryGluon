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

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IndexEntry;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.query.StreamingOperation;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.query.indexes.KeyIndex;
import com.basho.riak.client.RiakRetryFailedException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RiakDriver extends BasicStorageDriver {

    static class ValueType {
        public int type;
        public byte[] data;
    }

    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private Log m_log = null;
    private Bucket m_bucket;
    private IRiakClient m_pbClient = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public RiakDriver(String[] args) {
        super(args);
        m_log = LogFactory.getLog(RiakDriver.class); 
        setStopKeyMinus1(true);
    }

    @Override
    public StorageDriver createInstance() {
        StorageDriver driver = new RiakDriver(getArgs());
        driver.init();
        return driver;
    }

    @Override
    public boolean init() {
        try {
            m_pbClient = RiakFactory.pbcClient();
        } catch (RiakException e ) {
            m_log.error(e);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public void close() {
        if (m_pbClient != null) {
            m_pbClient.shutdown();
            m_pbClient = null;
        }
    }

    @Override
    public String getName() {
        return "Riak";
    }

    @Override
    public void setDatabase(String dbName) {
        try {
            m_bucket = m_pbClient.fetchBucket(dbName).execute();
        } catch (RiakRetryFailedException e) {
            throw new RuntimeException(e); // FIXME quickhack
        }
    }

    @Override
    public int addData(HistoryData history) {
        try {
            String key = makeKey(history.id, history.sec, history.ns);
            ValueType value = new ValueType();
            value.type = history.type;
            value.data = history.getDataAsByteArray();
            m_bucket.store(key, value).execute();
        } catch (RiakRetryFailedException e) {
            m_log.error(e);
            e.printStackTrace();
            return ErrorCode.RIAK_EXCEPTION;
        }
        return ErrorCode.SUCCESS;
    }

    @Override
    public boolean deleteDB() {
        try {
            Iterable<String> keys = m_bucket.keys();
            long count = 0;
            for (String keyName : keys) {
                m_bucket.delete(keyName).execute();
                count++;
            }
            m_log.info("Deleted " + count + " entries.");
        } catch (RiakException e) {
            m_log.error(e);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /* -----------------------------------------------------------------------
     * Protected Methods
     * -------------------------------------------------------------------- */
    @Override
    protected HistoryDataSet
      getDataSet(String startKey, String stopKey, long maxCount) {
        long count = 0;
        boolean countLimited = (maxCount != BridgeWorker.MAX_ENTRIES_UNLIMITED);
        HistoryDataSet dataSet = new HistoryDataSet();
        try {
            HistoryData history;
            StreamingOperation<IndexEntry> keyList =
              m_bucket.fetchIndex(KeyIndex.index).from(startKey).to(stopKey).executeStreaming();
            for (IndexEntry key : keyList) {
                ValueType value = m_bucket.fetch(key.getObjectKey(), ValueType.class).execute();
                if (value == null)
                    continue;
                history = getHistoryData(key.getObjectKey(), value);
                if (history == null)
                    continue;
                dataSet.add(history);
                count++;
            }

            // TODO: improve performance
            if (countLimited && count >= maxCount) {
                while (dataSet.size() > maxCount) {
                    Iterator<HistoryData> it = dataSet.descendingIterator();
                    dataSet.remove(it.next());
                }
            }

        } catch (RiakException e) {
            m_log.error(e);
            e.printStackTrace();
        }
        return dataSet;
    }

    @Override
    protected boolean deleteRow(HistoryData history, Object arg) {
        try {
            m_bucket.delete(history.key).execute();
        } catch (RiakException e) {
            m_log.error(e);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /* -----------------------------------------------------------------------
     * Private Methods
     * -------------------------------------------------------------------- */
    private HistoryData getHistoryData(String key, ValueType value) {
        HistoryData history = new HistoryData();
        history.key = key;
        fillItemIdClockNsWithKey(key, history);
        history.type = (short)value.type;
        history.data = value.data;
        if (!history.fixupData()) {
            m_log.warn("Failed: fixupData(): type:" + history.type);
            return null;
        }
        return history;
    }
}
