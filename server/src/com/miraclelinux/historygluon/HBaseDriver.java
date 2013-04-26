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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;

public class HBaseDriver extends BasicStorageDriver {

    /* -----------------------------------------------------------------------
     * Const Members
     * -------------------------------------------------------------------- */
    private static final String HISTORY_FAMILY_NAME = "history";
    private static final int NUM_QUALIFIERS = 5; // id, sec, ns, type, and data

    private static final byte[] BYTES_FAMILY = Bytes.toBytes("history");
    private static final byte[] BYTES_ID   = Bytes.toBytes("id");
    private static final byte[] BYTES_SEC  = Bytes.toBytes("sec");
    private static final byte[] BYTES_NS   = Bytes.toBytes("ns");
    private static final byte[] BYTES_TYPE = Bytes.toBytes("type");
    private static final byte[] BYTES_DATA = Bytes.toBytes("data");

    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private Log m_log = null;
    private Configuration m_config = null;
    private String m_tableName = null;
    private byte[] m_tableNameBytes = null;
    private HTable m_table = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public HBaseDriver(String[] args) {
        super(args);
        m_log = LogFactory.getLog(HBaseDriver.class);
        m_config = HBaseConfiguration.create();
    }

    @Override
    public StorageDriver createInstance() {
        StorageDriver driver = new HBaseDriver(getArgs());
        driver.init();
        return driver;
    }

    @Override
    public void close() {
        try {
            if (m_table != null) {
                m_table.close();
                m_table = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            m_log.error(e);
        }
        super.close();
    }

    @Override
    public String getName() {
        return "HBase";
    }

    @Override
    public void setDatabase(String dbName) {
        m_tableName = dbName;
        m_tableNameBytes = Bytes.toBytes(m_tableName);
        makeTableIfNeeded();
    }

    @Override
    public int addData(HistoryData history) {
        try {
            HTable table = getHTable();
            String key = String.format("%016x%08x%08x",
                                       history.id, history.sec,
                                       history.ns);
            Put putData = new Put(Bytes.toBytes(key));
            putData.add(BYTES_FAMILY, BYTES_ID,   Bytes.toBytes(history.id));
            putData.add(BYTES_FAMILY, BYTES_SEC,  Bytes.toBytes(history.sec));
            putData.add(BYTES_FAMILY, BYTES_NS,   Bytes.toBytes(history.ns));
            putData.add(BYTES_FAMILY, BYTES_TYPE, Bytes.toBytes(history.type));
            putData.add(BYTES_FAMILY, BYTES_DATA, history.getDataAsByteArray());
            table.put(putData);
        } catch (IOException e) {
            e.printStackTrace();
            m_log.error(e);
            return ErrorCode.HBASE_EXCEPTION;
        }
        return ErrorCode.SUCCESS;
    }

    @Override
    public boolean deleteDB() {
        m_log.error("HBaseDriver#deleteDB() is not implemented.");
        return false;
    }

    /* -----------------------------------------------------------------------
     * Protected Methods
     * -------------------------------------------------------------------- */
    @Override
    protected HistoryDataSet
      getDataSet(String startKey, String stopKey, long maxCount) {
        HistoryDataSet dataSet = new HistoryDataSet();
        ResultScanner resultScanner = null;
        boolean countLimited = (maxCount != BridgeWorker.MAX_ENTRIES_UNLIMITED);
        try {
            HTable table = getHTable();
            byte[] startRow = Bytes.toBytes(startKey);
            byte[] stopRow = Bytes.toBytes(stopKey);
            Scan scan = new Scan(startRow, stopRow);
            if (maxCount != BridgeWorker.MAX_ENTRIES_UNLIMITED) {
                if (maxCount > Integer.MAX_VALUE) {
                    String msg;
                    msg = "maxCount > Integer.MAX_VALUE: " + maxCount;
                    throw new InternalCheckException(msg);
                }
                scan.setBatch((int)maxCount * NUM_QUALIFIERS);
            }
            resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                buildHistoryData(table, result, dataSet);
                if (countLimited && (dataSet.size() >= maxCount))
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
            m_log.error(e);
        } finally {
            if (resultScanner != null)
                resultScanner.close();
        }
        return dataSet;
    }

    protected Object deleteRowsPreAction() {
        HTable table = null;
        try {
            table = new HTable(m_config, m_tableNameBytes);
        } catch (IOException e) {
            e.printStackTrace();
            m_log.error(e);
        }
        return table;
    }

    @Override
    protected boolean deleteRow(HistoryData history, Object arg) {
        try {
            HTable table = (HTable)arg;
            byte[] row = Bytes.toBytes(history.key);
            Delete del = new Delete(row);
            table.delete(del);
        } catch (IOException e) {
            e.printStackTrace();
            m_log.error("Failed to delete row: " + history.toString()
                        + ": " + e);
            return false;
        }
        return true;
    }

    /* -----------------------------------------------------------------------
     * Private Methods
     * -------------------------------------------------------------------- */
    private boolean makeTableIfNeeded() {
        try {
            HBaseAdmin admin = new HBaseAdmin(m_config);
            if (!admin.tableExists(m_tableNameBytes))
                makeTable(admin);
        }
        catch (MasterNotRunningException e) {
            e.printStackTrace();
            m_log.error(e);
            return false;
        }
        catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            m_log.error(e);
            return false;
        }
        catch (IOException e) {
            e.printStackTrace();
            m_log.error(e);
            return false;
        }
        return true;
    }

    private void makeTable(HBaseAdmin admin) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(m_tableNameBytes);
        desc.addFamily(new HColumnDescriptor(HISTORY_FAMILY_NAME));
        admin.createTable(desc);
    }

    private HTable getHTable() {
        if (m_table != null)
            return m_table;

        try {
            m_table = new HTable(m_config, m_tableNameBytes);
            m_table.setAutoFlush(false);
        } catch (IOException e) {
            e.printStackTrace();
            m_log.error(e);
        }

        return m_table;
    }

    private void buildHistoryData(HTable table, Result result,
                                  HistoryDataSet dataSet)
      throws IOException {

        // key
        HistoryData history = new HistoryData();

        // fill one value
        for (KeyValue keyVal : result.list()) {
            if (!obtainOneValue(keyVal, history))
                return;
        }

        if (!history.fixupData()) {
            m_log.warn("Failed: fixupData(): " + history.toString());
            return;
        }

        dataSet.add(history);
    }

    private boolean obtainOneValue(KeyValue keyVal, HistoryData history)
      throws IOException {
        // key
        int length = keyVal.getRowLength();
        byte[] buf = new byte[length];
        Bytes.putBytes(buf, 0, keyVal.getBuffer(),
                       keyVal.getRowOffset(), length);
        String key = Bytes.toString(buf, 0,length);
        if (history.key == null)
            history.key = key;
        else if (!history.key.equals(key)) {
            m_log.error("key is not matched: " + history.key + " : " + key);
            return false;
        }

        // qualifier
        length = keyVal.getQualifierLength();
        byte[] qualBuf = buf;
        if (qualBuf.length < length)
            qualBuf = new byte[length];
        Bytes.putBytes(qualBuf, 0, keyVal.getBuffer(),
                       keyVal.getQualifierOffset(), length);

        // value
        length = keyVal.getValueLength();
        buf = new byte[length];
        Bytes.putBytes(buf, 0, keyVal.getBuffer(),
                       keyVal.getValueOffset(), length);

        if (Utils.memEquals(qualBuf, BYTES_ID, BYTES_ID.length))
            history.id = Bytes.toLong(buf);
        else if (Utils.memEquals(qualBuf, BYTES_SEC, BYTES_SEC.length))
            history.sec = Bytes.toInt(buf);
        else if (Utils.memEquals(qualBuf, BYTES_NS, BYTES_NS.length))
            history.ns = Bytes.toInt(buf);
        else if (Utils.memEquals(qualBuf, BYTES_TYPE, BYTES_TYPE.length))
            history.type = Bytes.toShort(buf);
        else if (Utils.memEquals(qualBuf, BYTES_DATA, BYTES_DATA.length))
            history.data = buf;

        return true;
    }
}
