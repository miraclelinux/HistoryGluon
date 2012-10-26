package com.miraclelinux.historygluon;

import java.util.List;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
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
     * Private constant
     * -------------------------------------------------------------------- */
    private static final String BUCKET_NAME = "zabbix";

    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private Log m_log = null;
    private IRiakClient m_pbClient = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public RiakDriver() {
        m_log = LogFactory.getLog(RiakDriver.class); 
    }

    @Override
    public StorageDriver createInstance() {
        StorageDriver driver = new RiakDriver();
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
    public boolean addData(HistoryData history) {
        try {
            Bucket bucket = m_pbClient.fetchBucket(BUCKET_NAME).execute();
            String key = makeKey(history.id, history.sec, history.ns);
            ValueType value = new ValueType();
            value.type = history.type;
            value.data = history.getDataAsByteArray();
            bucket.store(key, value).execute();
        } catch (RiakRetryFailedException e) {
            m_log.error(e);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean deleteDB() {
        try {
            Bucket bucket = m_pbClient.fetchBucket(BUCKET_NAME).execute();
            Iterable<String> keys = bucket.keys();
            long count = 0;
            for (String keyName : keys) {
                bucket.delete(keyName).execute();
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
      getDataSet(long id, String startKey, String stopKey, int maxCount) {
        HistoryDataSet dataSet = new HistoryDataSet();
        try {
            Bucket bucket = m_pbClient.fetchBucket(BUCKET_NAME).execute();
            List<String> keyList =
              bucket.fetchIndex(KeyIndex.index).from(startKey).to(stopKey).execute();
            for (String key : keyList) {
                ValueType value = bucket.fetch(key, ValueType.class).execute();
                HistoryData history = getHistoryData(key, value);
                dataSet.add(history);
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
            Bucket bucket = m_pbClient.fetchBucket(BUCKET_NAME).execute();
            bucket.delete(history.key).execute();
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
        history.fixupData();
        return history;
    }
}
