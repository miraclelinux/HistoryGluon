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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CassandraDriver extends BasicStorageDriver {

    /* -----------------------------------------------------------------------
     * Private constant
     * -------------------------------------------------------------------- */
    private static final String COLUMN_FAMILY = "history";
    private static final String COLUMN_ITEM_ID = "id";
    private static final String COLUMN_CLOCK = "sec";
    private static final String COLUMN_NS = "ns";
    private static final String COLUMN_TYPE = "type";
    private static final String COLUMN_DATA = "data";

    private static final ByteBuffer
      BB_COLUMN_ITEM_ID = Utils.stringToByteBuffer(COLUMN_ITEM_ID);
    private static final ByteBuffer
      BB_COLUMN_CLOCK = Utils.stringToByteBuffer(COLUMN_CLOCK);
    private static final ByteBuffer
      BB_COLUMN_NS = Utils.stringToByteBuffer(COLUMN_NS);
    private static final ByteBuffer
      BB_COLUMN_TYPE = Utils.stringToByteBuffer(COLUMN_TYPE);
    private static final ByteBuffer
      BB_COLUMN_DATA = Utils.stringToByteBuffer(COLUMN_DATA);

    private static final int DEFALUT_PORT = 9160;
    private static final int COUNT_INTERNAL_LIMIT = 0x7fffffff;

    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private Log m_log = null;
    private String m_keySpace = null;
    private Cassandra.Client m_client = null;
    private TTransport m_transport = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public CassandraDriver() {
        m_log = LogFactory.getLog(CassandraDriver.class); 
        setStopKeyMinus1(true);
    }

    @Override
    public StorageDriver createInstance() {
        StorageDriver driver = new CassandraDriver();
        driver.init();
        return driver;
    }

    @Override
    public boolean init() {
        TSocket socket = new TSocket("localhost", DEFALUT_PORT);
        m_transport = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(m_transport);
        try {
            m_transport.open();
        } catch (TTransportException e) {
            m_log.error(e);
            e.printStackTrace();
            return false;
        }
        m_client = new Cassandra.Client(protocol);
        return true;
    }

    @Override
    public void close() {
        if (m_transport != null) {
            m_transport.close();
            m_transport = null;
        }
    }


    @Override
    public String getName() {
        return "Cassandra";
    }

    @Override
    public void setDatabase(String dbName) {
        m_keySpace = dbName;
        makeTableIfNeeded();
    }

    @Override
    public int addData(HistoryData history) {

        ByteBuffer dataBuffer = history.getDataAsByteBuffer();
        if (dataBuffer == null) {
            m_log.error("Unknown data type: " + history.type);
            return ErrorCode.INVALID_DATA_TYPE;
        }

        ColumnParent columnParent = new ColumnParent();
        columnParent.column_family = COLUMN_FAMILY;
        Column column = new Column();
        column.setTimestamp(System.currentTimeMillis());
        String key = makeKey(history.id, history.sec, history.ns);

        insertColumn(columnParent, column, key, BB_COLUMN_ITEM_ID, history.id);
        insertColumn(columnParent, column, key, BB_COLUMN_CLOCK, history.sec);
        insertColumn(columnParent, column, key, BB_COLUMN_NS, history.ns);
        insertColumn(columnParent, column, key, BB_COLUMN_TYPE, history.type);
        insertColumn(columnParent, column, key, BB_COLUMN_DATA, dataBuffer);

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
      getDataSet(long id, String startKey, String stopKey, int maxCount)
      throws HistoryDataSet.TooManyException {
        HistoryDataSet dataSet = new HistoryDataSet();

        KeyRange keyRange = new KeyRange();
        keyRange.setStart_key(Utils.stringToByteBuffer(startKey));
        keyRange.setEnd_key(Utils.stringToByteBuffer(stopKey));
        if (maxCount == COUNT_UNLIMITED)
            maxCount = COUNT_INTERNAL_LIMIT;
        keyRange.setCount(maxCount);

        // row filter
        List<IndexExpression> rowFilter = new ArrayList<IndexExpression>();
        IndexExpression idxExpr =
          new IndexExpression(BB_COLUMN_TYPE, IndexOperator.GTE, 
                              Utils.intToByteBuffer(0));
        rowFilter.add(idxExpr);
        keyRange.setRow_filter(rowFilter);

        ColumnParent columnParent = new ColumnParent(COLUMN_FAMILY);
        SlicePredicate predicate = new SlicePredicate();
        predicate.addToColumn_names(BB_COLUMN_CLOCK);
        predicate.addToColumn_names(BB_COLUMN_NS);
        predicate.addToColumn_names(BB_COLUMN_TYPE);
        predicate.addToColumn_names(BB_COLUMN_DATA);

        List<KeySlice> list;
        try {
            list = m_client.get_range_slices(columnParent, predicate,
                                             keyRange, ConsistencyLevel.ONE);
        } catch (Exception e) {
            m_log.error(e);
            e.printStackTrace();
            return null;
        }
        if (list.size() == COUNT_INTERNAL_LIMIT)
            throw new HistoryDataSet.TooManyException();

        for (KeySlice slice : list) {
            HistoryData history = new HistoryData();
            history.id = id;
            history.key = Utils.byteArrayToString(slice.getKey());
            List<ColumnOrSuperColumn> cols = slice.getColumns();
            for (ColumnOrSuperColumn cs : cols)
                obtainOneValue(cs, history);
            if (!history.fixupData()) {
                m_log.warn("Failed: fixupData(): type:" + history.type);
                continue;
            }
            dataSet.add(history);
        }

        return dataSet;
    }

    @Override
    protected boolean deleteRow(HistoryData history, Object arg) {
        ByteBuffer keyBB = Utils.stringToByteBuffer(history.key);
        long timestamp = System.currentTimeMillis();
        ColumnPath columnPath = new ColumnPath(COLUMN_FAMILY);
        try {
            m_client.remove(keyBB, columnPath, timestamp, ConsistencyLevel.ONE);
        } catch (Exception e) {
            m_log.error(e);
            e.printStackTrace();
            return false;
        }
        return true;
    }


    /* -----------------------------------------------------------------------
     * Private Methods
     * -------------------------------------------------------------------- */
    private boolean makeTableIfNeeded() {
        try {
            m_client.set_keyspace(m_keySpace);
            return true;
        } catch (InvalidRequestException e) {
            // In this point, it is possible that keyspace doesnot exist.
            // So try to make keyspace in the following.
        } catch (Exception e) {
            e.printStackTrace();
            m_log.error(e);
            return false;
        }

        if (!makeTable())
            return false;

        try {
            m_client.set_keyspace(m_keySpace);
        } catch (Exception e) {
            e.printStackTrace();
            m_log.error(e);
            return false;
        }
        return true;
    }

    private boolean makeTable() {
        try {
            KsDef ksDef = new KsDef();
            ksDef.name = m_keySpace;
            ksDef.strategy_class = "org.apache.cassandra.locator.SimpleStrategy";
            Map<String,String> strategyOptions = new HashMap<String,String>();
            strategyOptions.put("replication_factor", "1");
            ksDef.strategy_options = strategyOptions;
            CfDef cfDef = new CfDef(m_keySpace, COLUMN_FAMILY);
            cfDef.comparator_type = "UTF8Type";

            addColumn(cfDef, BB_COLUMN_ITEM_ID, "LongType", true);
            addColumn(cfDef, BB_COLUMN_CLOCK,   "Int32Type", true);
            addColumn(cfDef, BB_COLUMN_NS,      "Int32Type", false);
            addColumn(cfDef, BB_COLUMN_TYPE,    "Int32Type", false);
            addColumn(cfDef, BB_COLUMN_DATA,    "BytesType", false);

            ksDef.cf_defs = Arrays.asList(cfDef);
            m_client.system_add_keyspace(ksDef);
        } catch (Exception e) {
            e.printStackTrace();
            m_log.error(e);
            return false;
        }
        m_log.info("Created keyspace: " + m_keySpace);
        return true;
    }

    private void addColumn(CfDef cfDef, ByteBuffer nameByteBuffer,
                           String validationClass, boolean index) {
        ColumnDef columnDef = new ColumnDef(nameByteBuffer, validationClass);
        if (index)
            columnDef.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef);
    }

    private void insertColumn(ColumnParent columnParent, Column column,
                              String key, ByteBuffer name, ByteBuffer data) {
        column.name = name;
        column.value = data;
        try {
            m_client.insert(Utils.stringToByteBuffer(key),
                            columnParent, column, ConsistencyLevel.ONE);
        } catch (Exception e) {
            m_log.error(e);
            e.printStackTrace();
        }
    }

    private void insertColumn(ColumnParent columnParent, Column column,
                              String key, ByteBuffer name, long data) {
        insertColumn(columnParent, column, key, name,
                     Utils.longToByteBuffer(data));
    }

    private void insertColumn(ColumnParent columnParent, Column column,
                              String key, ByteBuffer name, double data) {
        insertColumn(columnParent, column, key, name,
                     Utils.doubleToByteBuffer(data));
    }

    private void insertColumn(ColumnParent columnParent, Column column,
                              String key, ByteBuffer name, int data) {
        insertColumn(columnParent, column, key, name,
                     Utils.intToByteBuffer(data));
    }

    private void insertColumn(ColumnParent columnParent, Column column,
                              String key, ByteBuffer name, String data) {
        insertColumn(columnParent, column, key, name,
                     Utils.stringToByteBuffer(data));
    }

    private void obtainOneValue(ColumnOrSuperColumn cs, HistoryData history) {
        Column column = cs.getColumn();
        String name = Utils.byteArrayToString(column.getName());
        byte[] value = column.getValue();
        if (name.equals(COLUMN_ITEM_ID))
            history.id = Utils.byteArrayToLong(value);
        else if (name.equals(COLUMN_CLOCK))
            history.sec = Utils.byteArrayToInt(value);
        else if (name.equals(COLUMN_NS))
            history.ns = Utils.byteArrayToInt(value);
        else if (name.equals(COLUMN_TYPE))
            history.type = (short)Utils.byteArrayToInt(value); // 'type' is stored as Int.
        else if (name.equals(COLUMN_DATA))
            history.data = value;
    }
}
