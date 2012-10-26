package com.miraclelinux.historygluon;

import java.lang.Comparable;
import java.nio.ByteBuffer;

public class HistoryData implements Comparable<HistoryData> {
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

    public static final int SEC_UNKNOWN = -1;

    // -----------------------------------------------------------------------
    // Public members
    // -----------------------------------------------------------------------
    public String key = null;
    public short type = TYPE_UNKNOWN;

    public long id = -1;
    public int sec = SEC_UNKNOWN;;
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
    public int compareTo(HistoryData history) {
        int secComparedResult = Utils.compareAsUnsigned(sec, history.sec);
        if (secComparedResult != 0)
            return secComparedResult;

        int nsComparedResult = Utils.compareAsUnsigned(ns, history.ns);
        if (nsComparedResult != 0)
            return nsComparedResult;

        int idComparedResult = Utils.compareAsUnsigned(id, history.id);
        if (idComparedResult != 0)
            return idComparedResult;

        return 0;
    }

    // -----------------------------------------------------------------------
    // Public method
    // -----------------------------------------------------------------------
    public HistoryData() {
    }

    public HistoryData(HistoryData history) {
        if (history.key != null)
            key = new String(history.key);
        type = history.type;

        id = history.id;
        sec = history.sec;
        ns = history.ns;

        dataUint64 = history.dataUint64;
        dataFloat = history.dataFloat;
        if (history.dataString != null)
           dataString = new String(history.dataString);

        // note: member 'data' is not copied
    }

    public boolean secIsValid() {
        return (sec != SEC_UNKNOWN);
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
        String str = String.format("<<HistoryData>> type: %d, id: %016x, sec: %08x, ns: %08x, %s", type, id, sec, ns, dataStr);
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

    public static int comparePreferId(HistoryData h0, HistoryData h1) {
        int idComparedResult = Utils.compareAsUnsigned(h0.id, h1.id);
        if (idComparedResult != 0)
            return idComparedResult;

        int secComparedResult = Utils.compareAsUnsigned(h0.sec, h1.sec);
        if (secComparedResult != 0)
            return secComparedResult;

        int nsComparedResult = Utils.compareAsUnsigned(h0.ns, h1.ns);
        if (nsComparedResult != 0)
            return nsComparedResult;

        return 0;
    }
}

