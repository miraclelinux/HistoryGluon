package com.miraclelinux.historygluon;

import java.lang.Comparable;
import java.nio.ByteBuffer;

public class HistoryData implements Comparable {
    // -----------------------------------------------------------------------
    // Public inner class
    // -----------------------------------------------------------------------
    public static class DataNotNumericException extends Exception {
    }

    // -----------------------------------------------------------------------
    // Public constant
    // -----------------------------------------------------------------------
    public static final int TYPE_UNKNOWN = -1;

    public static final int TYPE_FLOAT = 0;
    public static final int TYPE_STRING = 1;
    public static final int TYPE_UINT64 = 2;

    public static final int CLOCK_UNKNOWN = -1;

    // -----------------------------------------------------------------------
    // Public members
    // -----------------------------------------------------------------------
    public String key = null;
    public short type = TYPE_UNKNOWN;

    public long itemId = -1;
    public int clock = CLOCK_UNKNOWN;;
    public int ns = -1;

    // normally one of the following is used.
    public long dataUint64 = 0;
    public double dataFloat = 0;
    public String dataString = null;

    // used temporarily
    public byte[] data = null;

    // -----------------------------------------------------------------------
    // Override method
    // -----------------------------------------------------------------------
    @Override
    public int compareTo(Object obj) {
        HistoryData history = (HistoryData)obj;
        if (clock < history.clock)
            return -1;
        else if (clock > history.clock)
            return 1;

        if (ns < history.ns)
            return -1;
        else if (ns > history.ns)
            return 1;

        if (itemId < history.itemId)
            return -1;
        else if (itemId > history.itemId)
            return 1;

        return 0;
    }

    // -----------------------------------------------------------------------
    // Public method
    // -----------------------------------------------------------------------
    public HistoryData() {
    }

    public HistoryData(HistoryData history) {
        key = new String(history.key);
        type = history.type;

        itemId = history.itemId;
        clock = history.clock;
        ns = history.ns;

        dataUint64 = history.dataUint64;
        dataFloat = history.dataFloat;
        String dataString = new String(history.dataString);

        // note: member 'data' is not copied
    }

    public boolean clockIsValid() {
        return (clock != CLOCK_UNKNOWN);
    }

    public double getDataAsDouble() throws DataNotNumericException {
        if (type == TYPE_FLOAT)
            return dataFloat;
        if (type == TYPE_UINT64)
            return Utils.toDoubleAsUnsigned(dataUint64);
        throw new DataNotNumericException();
    }

    public String toString() {
        String dataStr = new String();
        if (type == TYPE_FLOAT)
            dataStr = "data (Float): " + dataFloat;
        else if (type == TYPE_STRING)
            dataStr = "data (String): " + dataString;
        else if (type == TYPE_UINT64)
            dataStr = "data (Uint64): " + dataUint64;
        else
            dataStr = "data: Unknown";
        String str = String.format("<<HistoryData>> type: %d, itemId: %016x, clock: %08x, ns: %08x, %s", type, itemId, clock, ns, dataStr);
        return str;
    }

    public ByteBuffer getDataAsByteBuffer() {
        if (type == TYPE_FLOAT)
            return Utils.doubleToByteBuffer(dataFloat);
        else if (type == TYPE_UINT64)
            return Utils.longToByteBuffer(dataUint64);
        else if (type == TYPE_STRING)
            return Utils.stringToByteBuffer(dataString);
        return null;
    }

    public byte[] getDataAsByteArray() {
        return getDataAsByteBuffer().array();
    }

    public boolean fixupData() {
        if (type == TYPE_FLOAT)
            dataFloat = Utils.byteArrayToDouble(data);
        else if (type == TYPE_STRING)
            dataString = Utils.byteArrayToString(data);
        else if (type == TYPE_UINT64)
            dataUint64 = Utils.byteArrayToLong(data);
        else
            return false;
        return true;
    }
}

