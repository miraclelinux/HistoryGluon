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
import java.util.Iterator;

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
    private static final String QUALIFIER_DATA = "data";

    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private Log m_log = null;
    private Configuration m_config = null;
    private String m_tableName = null;
    private byte[] m_tableNameBytes = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public HBaseDriver() {
        m_log = LogFactory.getLog(HBaseDriver.class);
        m_config = HBaseConfiguration.create();
    }

    @Override
    public StorageDriver createInstance() {
        StorageDriver driver = new HBaseDriver();
        driver.init();
        return driver;
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
            HTable table = new HTable(m_config, m_tableNameBytes);
            String key = String.format("%016x%08x%08x",
                                       history.id, history.sec,
                                       history.ns);
            Put putData = new Put(Bytes.toBytes(key));
            byte[] familyBytes = Bytes.toBytes(HISTORY_FAMILY_NAME);

            putData.add(familyBytes,
                        Bytes.toBytes("id"),
                        Bytes.toBytes(history.id));
            putData.add(familyBytes,
                        Bytes.toBytes("sec"),
                        Bytes.toBytes(history.sec));
            putData.add(familyBytes,
                        Bytes.toBytes("ns"),
                        Bytes.toBytes(history.ns));
            putData.add(familyBytes,
                        Bytes.toBytes("type"),
                        Bytes.toBytes(history.type));

            byte[] qualifierBytes = Bytes.toBytes(QUALIFIER_DATA);
            byte[] dataBytes = history.getDataAsByteArray();
            putData.add(familyBytes, qualifierBytes, dataBytes);
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
      getDataSet(long id, String startKey, String stopKey, int maxCount) {
        HistoryDataSet dataSet = new HistoryDataSet();
        ResultScanner resultScanner = null;
        try {
            HTable table = new HTable(m_config, m_tableNameBytes);
            byte[] startRow = Bytes.toBytes(startKey);
            byte[] stopRow = Bytes.toBytes(stopKey);
            Scan scan = new Scan(startRow, stopRow);
            resultScanner = table.getScanner(scan);
            long count = 0;
            for (Result result : resultScanner) {
                buildHistoryData(table, result, dataSet, id);
                count++;
                if (count == maxCount)
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

    private void buildHistoryData(HTable table, Result result,
                                  HistoryDataSet dataSet, long id)
      throws IOException {

        // key and id
        HistoryData history = new HistoryData();
        history.id = id;

        // fill one value
        for (KeyValue keyVal : result.list()) {
            if (!obtainOneValue(keyVal, history))
                return;
        }

        if (!history.fixupData()) {
            m_log.warn("Failed: fixupData(): type:" + history.type);
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
        if (buf.length < length)
            buf = new byte[length];
        Bytes.putBytes(buf, 0, keyVal.getBuffer(),
                       keyVal.getQualifierOffset(), length);
        String qualifier = Bytes.toString(buf, 0, length);

        // value
        length = keyVal.getValueLength();
        buf = new byte[length];
        Bytes.putBytes(buf, 0, keyVal.getBuffer(),
                       keyVal.getValueOffset(), length);

        if (qualifier.equals("id"))
            history.id = Bytes.toLong(buf); 
        else if (qualifier.equals("sec"))
            history.sec = Bytes.toInt(buf); 
        else if (qualifier.equals("ns"))
            history.ns = Bytes.toInt(buf); 
        else if (qualifier.equals("type"))
            history.type = Bytes.toShort(buf);
        else if (qualifier.equals(QUALIFIER_DATA))
            history.data = buf;

        return true;
    }
}
