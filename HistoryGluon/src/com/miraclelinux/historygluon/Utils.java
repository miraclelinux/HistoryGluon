package com.miraclelinux.historygluon;

import java.nio.ByteBuffer;
import java.io.UnsupportedEncodingException;

public class Utils {

    public static double toDoubleAsUnsigned(long v) {
        if (v >= 0)
            return (double)v;
        double dbl = v;
        dbl += Math.pow(2, 64);
        return dbl;
    }

    public static int compareAsUnsigned(int v0, int v1) {
        long lv0 = v0;
        long lv1 = v1;
        if (lv0 < 0)
            lv0 += 0x100000000L;
        if (lv1 < 0)
            lv1 += 0x100000000L;

        if (lv0 < lv1)
            return -1;
        else if (lv0 == lv1)
            return 0;
        else
            return 1;
    }

    public static int compareAsUnsigned(long v0, long v1) {
        if (v0 >= 0 && v1 < 0)
            return -1;

        if (v0 < 0 && v1 >= 0)
            return 1;

        // where, lv0 and lv1 must be both positive or both negative.
        if (v0 < v1)
            return -1;
        else if (v0 == v1)
            return 0;
        else
            return 1;
    }

    public static short byteArrayToShort(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getShort();
    }

    public static int byteArrayToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static long byteArrayToLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    public static double byteArrayToDouble(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getDouble();
    }

    public static ByteBuffer longToByteBuffer(long val) {
        return ByteBuffer.allocate(8).putLong(0, val);
    }

    public static ByteBuffer intToByteBuffer(int val) {
        return ByteBuffer.allocate(4).putInt(0, val);
    }

    public static ByteBuffer doubleToByteBuffer(double val) {
        return ByteBuffer.allocate(8).putDouble(0, val);
    }

    public static ByteBuffer stringToByteBuffer(String string) {
        ByteBuffer bb = null;
        try {
            bb = ByteBuffer.wrap(string.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new Error();
        }
        return bb;
    }

    public static String byteArrayToString(byte[] bytes) {
        String string = null;
        try {
            string = new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new Error();
        }
        return string;
    }
}
