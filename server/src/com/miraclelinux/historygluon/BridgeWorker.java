package com.miraclelinux.historygluon;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BridgeWorker extends Thread {

    /* -----------------------------------------------------------------------
     * Private constant
     * -------------------------------------------------------------------- */
    private static final int PKT_SIZE_LENGTH = 4;
    private static final int PKT_TYPE_LENGTH = 2;
    private static final int PKT_ITEM_ID_LENGTH = 8;
    private static final int PKT_CLOCK_LENGTH = 4;
    private static final int PKT_NS_LENGTH = 4;
    private static final int PKT_DATA_TYPE_LENGTH = 2;
    private static final int PKT_DATA_FLOAT_LENGTH = 8;
    private static final int PKT_DATA_UINT64_LENGTH = 8;
    private static final int PKT_DATA_STRING_SIZE_LENGTH = 4;
    private static final int PKT_DATA_BLOB_SIZE_LENGTH = 4;
    private static final int PKT_NUM_ENTRIES_LENGTH = 4;
    private static final int PKT_SORT_ORDER_LENGTH = 2;
    private static final int PKT_SEARCH_NEAR_LENGTH = 1;

    private static final short PKT_TYPE_ADD_DATA       = 100;
    private static final short PKT_TYPE_GET            = 1000;
    private static final short PKT_TYPE_GET_WITH_TS    = 1050;
    private static final short PKT_TYPE_GET_MIN_CLOCK  = 1100;
    private static final short PKT_TYPE_GET_STATISTICS = 1200;
    private static final short PKT_TYPE_DELETE         = 2000;

    private static final short PKT_SORT_ORDER_ASCENDING = 0;
    private static final short PKT_SORT_ORDER_DESCENDING = 1;

    private static final int REPLY_RESULT_LENGTH = 4;

    private static final int RESULT_SUCCESS = 0;
    private static final int RESULT_ERROR_UNKNOWN_REASON = 1;
    private static final int RESULT_ERROR_TOO_MANY_RECORDS = 2;
    private static final int RESULT_ERROR_NO_DATA = 3;

    private static final int MAX_ENTRIES_NO_LIMIT = 0;

    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private Socket m_socket = null;
    private Log m_log = null;
    private BufferedOutputStream m_ostream = null;
    private BufferedInputStream m_istream = null;
    private StorageDriver m_driver = null;
    private ByteBuffer m_byteBuffer = null;
    private ExecTimeObserver m_cmdProcTimeObserver = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public BridgeWorker(Socket socket, StorageDriver driver) {
        m_socket = socket;
        m_log = LogFactory.getLog(BridgeWorker.class); 
        m_log.info("start BridgeWorker: host: " +
                   m_socket.getInetAddress().getHostAddress() +
                   ", remote port: " + m_socket.getPort());
        m_driver = driver;
        m_byteBuffer = ByteBuffer.allocate(100);
        m_byteBuffer.order(ByteOrder.LITTLE_ENDIAN); 
    }

    @Override
    public void run() {
        // make a time observer. This object should be created here,
        // because it uses thread information in it.
        m_cmdProcTimeObserver = new ExecTimeObserver("cmd");
        String env_measure_cmd_proc_time = System.getenv("MEASURE_CMD_PROC_TIME");
        if (env_measure_cmd_proc_time != null &&
            env_measure_cmd_proc_time.equals("1"))
            m_cmdProcTimeObserver.setEnable();

        try {
            m_ostream = new BufferedOutputStream(m_socket.getOutputStream());
            m_istream = new BufferedInputStream(m_socket.getInputStream());
            while (doHandshake());
        } catch (Exception e) {
            e.printStackTrace();
            m_log.error(e);
        } finally {
            m_driver.close();
            try {
                m_socket.close();
            } catch (IOException e) {
                m_log.error("failed to close socket: " + e);
            }
        }
    }

    /* -----------------------------------------------------------------------
     * Private Methods
     * -------------------------------------------------------------------- */
    private boolean doHandshake() throws IOException {

        // packet size
        byte[] sizeBuf = new byte[PKT_SIZE_LENGTH];
        if (m_istream.read(sizeBuf, 0, PKT_SIZE_LENGTH) == -1) {
            return false;
        }
        m_byteBuffer.clear();
        m_byteBuffer.put(sizeBuf);
        int pktSize = m_byteBuffer.getInt(0);

        // read pkt body
        byte[] pktBuf = new byte[pktSize];
        if (m_istream.read(pktBuf, 0, pktSize) == -1) {
            m_log.error("Unexpectedly input stream reaches the end.");
            return false;
        }

        // start measurement
        m_cmdProcTimeObserver.start();

        // get command type
        m_byteBuffer.clear();
        m_byteBuffer.put(pktBuf, 0, PKT_TYPE_LENGTH);
        int idx = 0;
        short type = m_byteBuffer.getShort(idx);
        idx += PKT_TYPE_LENGTH;

        // do processing
        boolean ret = false;
        if (type == PKT_TYPE_ADD_DATA)
            ret = addHistoryData(pktBuf, idx);
        else if (type == PKT_TYPE_GET)
            ret = getHistoryData(pktBuf, idx);
        else if (type == PKT_TYPE_GET_WITH_TS)
            ret = getHistoryDataWithTimestamp(pktBuf, idx);
        else if (type == PKT_TYPE_GET_MIN_CLOCK)
            ret = getMinClock(pktBuf, idx);
        else if (type == PKT_TYPE_GET_STATISTICS)
            ret = getStatistics(pktBuf, idx);
        else if (type == PKT_TYPE_DELETE)
            ret = deleteData(pktBuf, idx);
        else
            m_log.error("Got unknown command type: " + type);

        // stop measurement and log it
        m_cmdProcTimeObserver.stopAndLog("type: " + type);
        return ret;
    }

    private boolean putBufferWithCheckLength(byte[] pktBuf, int idx, int putLength) {
        if (pktBuf.length -idx < putLength) {
            m_log.error("packet length is too short: " + (pktBuf.length - idx));
            return false;
        }
        m_byteBuffer.put(pktBuf, idx, putLength);
        return true;
    }

    private boolean addHistoryData(byte[] pktBuf, int idx) throws IOException {
        HistoryData history = new HistoryData();

        int putLength = PKT_DATA_TYPE_LENGTH + PKT_ITEM_ID_LENGTH
                        + PKT_CLOCK_LENGTH + PKT_NS_LENGTH;
        if (!putBufferWithCheckLength(pktBuf, idx, putLength)) {
            // TODO: return error
            return false;
        }

        // data type, Item ID, sec, and ns
        history.type = m_byteBuffer.getShort(idx);
        idx += PKT_DATA_TYPE_LENGTH;

        history.id = m_byteBuffer.getLong(idx);
        idx += PKT_ITEM_ID_LENGTH;

        history.sec = m_byteBuffer.getInt(idx);
        idx += PKT_CLOCK_LENGTH;

        history. ns = m_byteBuffer.getInt(idx);
        idx += PKT_NS_LENGTH;

        // parse each data
        if (history.type == HistoryData.TYPE_FLOAT)
            procFloatData(pktBuf, idx, history);
        else if (history.type == HistoryData.TYPE_STRING)
            procStringData(pktBuf, idx, history);
        else if (history.type == HistoryData.TYPE_UINT64)
            procUint64Data(pktBuf, idx, history);
        else if (history.type == HistoryData.TYPE_BLOB)
            procBlobData(pktBuf, idx, history);
        else {
            // TODO: return error
            m_log.error("Got unknown data type: " + history.type);
            return false;
        }

        m_log.debug(history.toString());
        if (!m_driver.addData(history)) {
            m_log.error("Failed to add data.");
            // TODO: return error
            return false;
        }

        // write reply to the socket
        int length = PKT_TYPE_LENGTH + REPLY_RESULT_LENGTH;
        m_byteBuffer.clear();
        m_byteBuffer.putInt(length);
        m_byteBuffer.putShort(PKT_TYPE_ADD_DATA);
        m_byteBuffer.putInt(RESULT_SUCCESS);
        m_ostream.write(m_byteBuffer.array(), 0, m_byteBuffer.position());
        m_ostream.flush();

        return true;
    }

    private boolean getHistoryData(byte[] pktBuf, int idx) throws IOException {
        int putLength = PKT_ITEM_ID_LENGTH + 2 * PKT_CLOCK_LENGTH
                        + PKT_NUM_ENTRIES_LENGTH + PKT_SORT_ORDER_LENGTH;
        if (!putBufferWithCheckLength(pktBuf, idx, putLength))
            return false;

        // Item ID, sec0, and sec1
        long id = m_byteBuffer.getLong(idx);
        idx += PKT_ITEM_ID_LENGTH;

        int sec0 = m_byteBuffer.getInt(idx);
        idx += PKT_CLOCK_LENGTH;

        int sec1 = m_byteBuffer.getInt(idx);
        idx += PKT_CLOCK_LENGTH;

        int maxEntries = m_byteBuffer.getInt(idx);
        idx += PKT_NUM_ENTRIES_LENGTH;

        char sortOrder = m_byteBuffer.getChar(idx);
        idx += PKT_SORT_ORDER_LENGTH;

        HistoryDataSet dataSet = null;
        try {
            dataSet = m_driver.getData(id, sec0, sec1);
        } catch (HistoryDataSet.TooManyException e) {
            // FIXME: return the error
            return false;
        }

        // calcurate total size
        Iterator<HistoryData> it;
        if (sortOrder == PKT_SORT_ORDER_ASCENDING)
            it = dataSet.iterator();
        else if (sortOrder == PKT_SORT_ORDER_DESCENDING)
            it = dataSet.descendingIterator();
        else {
            m_log.error("Unknown sort order: " + sortOrder);
            return false;
        }

        int totalSize = PKT_TYPE_LENGTH + REPLY_RESULT_LENGTH
                        + PKT_NUM_ENTRIES_LENGTH;
        int numEntries = 0;
        while (it.hasNext()) {
            HistoryData history = it.next();
            totalSize += calcReplyPktSize(history);
            numEntries++;
            if (maxEntries != MAX_ENTRIES_NO_LIMIT && numEntries == maxEntries)
                break;
        }

        int commonDataSize = PKT_ITEM_ID_LENGTH
                             + PKT_CLOCK_LENGTH + PKT_NS_LENGTH
                             + PKT_DATA_TYPE_LENGTH;
        totalSize += numEntries * commonDataSize;

        // write reply header to the socket
        m_byteBuffer.clear();
        m_byteBuffer.putInt(totalSize);
        m_byteBuffer.putShort(PKT_TYPE_GET);
        m_byteBuffer.putInt(RESULT_SUCCESS);
        m_byteBuffer.putInt(numEntries);
        m_ostream.write(m_byteBuffer.array(), 0, m_byteBuffer.position());

        // write data set
        it = dataSet.iterator();
        while (it.hasNext()) {
            HistoryData history = it.next();
            sendOneHistoryData(history);
        }
        m_ostream.flush();

        return true;
    }

    private boolean getHistoryDataWithTimestamp(byte[] pktBuf, int idx) throws IOException {
        int putLength = PKT_ITEM_ID_LENGTH + PKT_CLOCK_LENGTH
                        + PKT_NS_LENGTH + PKT_SEARCH_NEAR_LENGTH;
        if (!putBufferWithCheckLength(pktBuf, idx, putLength))
            return false;

        // Item ID, sec0, and sec1
        long id = m_byteBuffer.getLong(idx);
        idx += PKT_ITEM_ID_LENGTH;

        int sec = m_byteBuffer.getInt(idx);
        idx += PKT_CLOCK_LENGTH;

        int ns = m_byteBuffer.getInt(idx);
        idx += PKT_NS_LENGTH;

        boolean searchNear = (m_byteBuffer.getChar(idx) == 0x01);
        idx += PKT_SEARCH_NEAR_LENGTH;

        HistoryData history = null;
        try {
            history = m_driver.getDataWithTimestamp(id, sec,
                                                    ns, searchNear);
        } catch (HistoryDataSet.TooManyException e) {
            // FIXME: return the error
            return false;
        }
        int numEntries = (history == null) ? 0 : 1;

        // write reply to the socket
        int length = PKT_TYPE_LENGTH + REPLY_RESULT_LENGTH +
                     PKT_NUM_ENTRIES_LENGTH;
        if (history != null) {
            length += PKT_ITEM_ID_LENGTH + PKT_CLOCK_LENGTH * 2 +
                      PKT_DATA_TYPE_LENGTH + calcReplyPktSize(history);
        }
        m_byteBuffer.clear();
        m_byteBuffer.putInt(length);
        m_byteBuffer.putShort(PKT_TYPE_GET_WITH_TS);
        m_byteBuffer.putInt(RESULT_SUCCESS);
        m_byteBuffer.putInt(numEntries);
        m_ostream.write(m_byteBuffer.array(), 0, m_byteBuffer.position());
        if (history != null)
            sendOneHistoryData(history);
        m_ostream.flush();
        return true;
    }

    private boolean getMinClock(byte[] pktBuf, int idx) throws IOException {

        int putLength = PKT_ITEM_ID_LENGTH;
        if (!putBufferWithCheckLength(pktBuf, idx, putLength))
            return false;

        // extract Item ID
        long id = m_byteBuffer.getLong(idx);
        idx += PKT_ITEM_ID_LENGTH;

        // get boundary of sec
        HistoryData history = null;
        try {
            history = m_driver.getDataWithMinimumClock(id);
        } catch (HistoryDataSet.TooManyException e) {
            // FIXME: return the error
            return false;
        }

        int minClock = 0;
        if (history != null)
            minClock = history.sec;

        // write reply to the socket
        int length = PKT_TYPE_LENGTH + REPLY_RESULT_LENGTH + PKT_CLOCK_LENGTH;
        m_byteBuffer.clear();
        m_byteBuffer.putInt(length);
        m_byteBuffer.putShort(PKT_TYPE_GET_MIN_CLOCK);
        m_byteBuffer.putInt(RESULT_SUCCESS);
        m_byteBuffer.putInt(minClock);
        m_ostream.write(m_byteBuffer.array(), 0, m_byteBuffer.position());
        m_ostream.flush();

        return true;
    }

    private boolean getStatistics(byte[] pktBuf, int idx) throws IOException {

        int putLength = PKT_ITEM_ID_LENGTH + PKT_CLOCK_LENGTH * 2;
        if (!putBufferWithCheckLength(pktBuf, idx, putLength))
            return false;

        // extract Item ID and secs
        long id = m_byteBuffer.getLong(idx);
        idx += PKT_ITEM_ID_LENGTH;

        int sec0 = m_byteBuffer.getInt(idx);
        idx += PKT_CLOCK_LENGTH;
        int sec1 = m_byteBuffer.getInt(idx);
        idx += PKT_CLOCK_LENGTH;

        Statistics statistics = null;
        try {
            statistics = m_driver.getStatistics(id, sec0, sec1);
        } catch (HistoryDataSet.TooManyException e) {
            // TODO: return error
            return false;
        }
        if (statistics == null) {
            // TODO: return error
            return false;
        }

        // write reply
        int length = PKT_TYPE_LENGTH + REPLY_RESULT_LENGTH +
                     PKT_ITEM_ID_LENGTH + PKT_CLOCK_LENGTH * 2 +
                     PKT_DATA_FLOAT_LENGTH * 3 + PKT_DATA_UINT64_LENGTH;
        m_byteBuffer.clear();
        m_byteBuffer.putInt(length);
        m_byteBuffer.putShort(PKT_TYPE_GET_STATISTICS);
        m_byteBuffer.putInt(RESULT_SUCCESS);
        m_byteBuffer.putLong(id);
        m_byteBuffer.putInt(statistics.sec0);
        m_byteBuffer.putInt(statistics.sec1);
        m_byteBuffer.putLong(statistics.count);
        m_byteBuffer.putDouble(statistics.min);
        m_byteBuffer.putDouble(statistics.max);
        m_byteBuffer.putDouble(statistics.sum);

        m_ostream.write(m_byteBuffer.array(), 0, m_byteBuffer.position());
        m_ostream.flush();

        return true;
    }

    private boolean deleteData(byte[] pktBuf, int idx) throws IOException {

        int putLength = PKT_ITEM_ID_LENGTH + PKT_CLOCK_LENGTH;
        if (!putBufferWithCheckLength(pktBuf, idx, putLength))
            return false;

        // extract Item ID and thresClock
        long id = m_byteBuffer.getLong(idx);
        idx += PKT_ITEM_ID_LENGTH;

        int thresClock = m_byteBuffer.getInt(idx);
        idx += PKT_CLOCK_LENGTH;

        // get min sec
        int numDeleted = m_driver.delete(id, thresClock);

        // write reply to the socket
        int length = PKT_TYPE_LENGTH + REPLY_RESULT_LENGTH + PKT_NUM_ENTRIES_LENGTH;
        m_byteBuffer.clear();
        m_byteBuffer.putInt(length);
        m_byteBuffer.putShort(PKT_TYPE_DELETE);
        m_byteBuffer.putInt(RESULT_SUCCESS);
        m_byteBuffer.putInt(numDeleted);
        m_ostream.write(m_byteBuffer.array(), 0, m_byteBuffer.position());
        m_ostream.flush();

        return true;
    }

    private long procFloatData(byte[] pktBuf, int idx, HistoryData history) {
        m_byteBuffer.clear();
        m_byteBuffer.put(pktBuf, idx, PKT_DATA_FLOAT_LENGTH);
        idx += PKT_DATA_FLOAT_LENGTH;
        history.dataFloat = m_byteBuffer.getDouble(0);
        return idx;
    }

    private long procStringData(byte[] pktBuf, int idx, HistoryData history) {
        m_byteBuffer.clear();
        m_byteBuffer.put(pktBuf, idx, PKT_DATA_STRING_SIZE_LENGTH);
        idx += PKT_DATA_STRING_SIZE_LENGTH;
        int stringLength = m_byteBuffer.getInt(0);
        history.dataString = new String(pktBuf, idx, stringLength);
        idx += stringLength;
        return idx;
    }

    private int procUint64Data(byte[] pktBuf, int idx, HistoryData history) {
        m_byteBuffer.clear();
        m_byteBuffer.put(pktBuf, idx, PKT_DATA_UINT64_LENGTH);
        idx += PKT_DATA_UINT64_LENGTH;
        history.dataUint64 = m_byteBuffer.getLong(0);
        return idx;
    }

    private long procBlobData(byte[] pktBuf, int idx, HistoryData history) {
        m_byteBuffer.clear();
        m_byteBuffer.put(pktBuf, idx, PKT_DATA_BLOB_SIZE_LENGTH);
        idx += PKT_DATA_BLOB_SIZE_LENGTH;
        // TODO: Suport large size
        int length = (m_byteBuffer.getInt(0));
        history.dataBlob = new byte[length];
        idx += length;
        return idx;
    }

    private int calcReplyPktSize(HistoryData history) {
        int size = 0;
        if (history.type == HistoryData.TYPE_FLOAT)
            size += PKT_DATA_FLOAT_LENGTH;
        else if (history.type == HistoryData.TYPE_STRING) {
            size += PKT_DATA_STRING_SIZE_LENGTH;
            size += history.dataString.length();
        } else if (history.type == HistoryData.TYPE_UINT64)
            size += PKT_DATA_UINT64_LENGTH;
        else if (history.type == HistoryData.TYPE_BLOB) {
            size += PKT_DATA_STRING_SIZE_LENGTH;
            size += history.dataBlob.length;
        } else
            m_log.error("Unknown history type:" + history.type);
        return size;
    }

    private void sendOneHistoryData(HistoryData history) throws IOException {
        m_byteBuffer.clear();
        m_byteBuffer.putLong(history.id);
        m_byteBuffer.putInt(history.sec);
        m_byteBuffer.putInt(history.ns);
        m_byteBuffer.putShort(history.type);

        if (history.type == HistoryData.TYPE_FLOAT)
            m_byteBuffer.putDouble(history.dataFloat);
        else if (history.type == HistoryData.TYPE_STRING)
            m_byteBuffer.putInt(history.dataString.length());
        else if (history.type == HistoryData.TYPE_UINT64)
            m_byteBuffer.putLong(history.dataUint64);
        else if (history.type == HistoryData.TYPE_BLOB)
            m_byteBuffer.putInt(history.dataBlob.length);
        else
            m_log.error("Unknown history type:" + history.type);

        // Send data (except string body and blob)
        m_ostream.write(m_byteBuffer.array(), 0, m_byteBuffer.position());

        // String body and Blob
        if (history.type == HistoryData.TYPE_STRING) {
            m_ostream.write(history.dataString.getBytes(), 0,
                            history.dataString.length());
        } else if (history.type == HistoryData.TYPE_BLOB)
            m_ostream.write(history.dataBlob, 0, history.dataBlob.length);
    }
}
