package com.miraclelinux.historygluon;

import org.junit.Test;
import static org.junit.Assert.*;

public class BasicStorageDriverTest extends BasicStorageDriver
{
    @Test
    public void testMakeKeyNormal() {
        long id = 0x123456789abcdef0L;
        int sec = 0x12345678;
        int ns = 999999999;
        String expect = String.format("%08x%04x%04x", id, sec, ns);
        assertEquals(expect, makeKey(id, sec, ns));
    }

    @Test
    public void testMakeKeyIncrement() {
        long id = 0x123456789abcdef0L;
        int sec = 0x12345678;
        int ns = 999999999;
        ns += 2;
        String expect = String.format("%016x%08x%08x", id, sec+1, 1);
        assertEquals(expect, makeKey(id, sec, ns));
    }

    @Test
    public void testMakeKeyIncrement2() {
        long id = 0x123456789abcdef0L;
        int sec = 0x12345678;
        int ns = 2123456789;
        String expect = String.format("%016x%08x%08x", id, sec+2, 123456789);
        assertEquals(expect, makeKey(id, sec, ns));
    }

    // Override methods
    @Override
    protected boolean deleteRow(HistoryData history, Object arg) {
        return true;
    }

    @Override
    protected HistoryDataSet
      getDataSet(long id, String startKey, String stopKey, int maxCount) {
        return null;
    }

    @Override
    public boolean deleteDB() {
        return true;
    }

    @Override
    public boolean addData(HistoryData history) {
        return true;
    }

    @Override
    public String getName() {
        return "BasicStorageDriverTest";
    }

    @Override
    public StorageDriver createInstance() {
        return this;
    }
}
