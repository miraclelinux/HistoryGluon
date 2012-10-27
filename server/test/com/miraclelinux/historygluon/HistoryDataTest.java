package com.miraclelinux.historygluon;

import org.junit.Test;
import static org.junit.Assert.*;

public class HistoryDataTest
{
    @Test
    public void testCompareToEq() {
        HistoryData h0;
        HistoryData h1;
        h0 = createHistoryData(1, 0, 0);
        h1 = createHistoryData(1, 0, 0);
        assertEquals(0, h0.compareTo(h1));

        h0 = createHistoryData(1, 0x01234567, 0x01234567);
        h1 = createHistoryData(1, 0x01234567, 0x01234567);
        assertEquals(0, h0.compareTo(h1));

        h0 = createHistoryData(1, 0x7fffffff, 0x7fffffff);
        h1 = createHistoryData(1, 0x7fffffff, 0x7fffffff);
        assertEquals(0, h0.compareTo(h1));

        h0 = createHistoryData(1, 0x80000000, 0x80000000);
        h1 = createHistoryData(1, 0x80000000, 0x80000000);
        assertEquals(0, h0.compareTo(h1));

        h0 = createHistoryData(1, 0xffffffff, 0xffffffff);
        h1 = createHistoryData(1, 0xffffffff, 0xffffffff);
        assertEquals(0, h0.compareTo(h1));
    }

    @Test
    public void testCompareToLess() {
        HistoryData h0;
        HistoryData h1;
        h0 = createHistoryData(1, 0x00000000, 0x00000000);
        h1 = createHistoryData(1, 0x01234567, 0x01234567);
        assertTrue(h0.compareTo(h1) < 0);

        h0 = createHistoryData(1, 0x00000000, 0x00000000);
        h1 = createHistoryData(1, 0x7fffffff, 0xffffffff);
        assertTrue(h0.compareTo(h1) < 0);

        h0 = createHistoryData(1, 0x7fffffff, 0x00000000);
        h1 = createHistoryData(1, 0x7fffffff, 0x00000001);
        assertTrue(h0.compareTo(h1) < 0);

        h0 = createHistoryData(1, 0x7fffffff, 0x7fffffff);
        h1 = createHistoryData(1, 0x7fffffff, 0x80000000);
        assertTrue(h0.compareTo(h1) < 0);

        h0 = createHistoryData(1, 0x00000000, 0x00000000);
        h1 = createHistoryData(1, 0x80000000, 0x00000000);
        assertTrue(h0.compareTo(h1) < 0);

        h0 = createHistoryData(1, 0x00000000, 0x00000000);
        h1 = createHistoryData(1, 0xffffffff, 0xffffffff);
        assertTrue(h0.compareTo(h1) < 0);

        h0 = createHistoryData(1, 0x7fffffff, 0xffffffff);
        h1 = createHistoryData(1, 0x80000000, 0x00000000);
        assertTrue(h0.compareTo(h1) < 0);

        h0 = createHistoryData(1, 0x80000000, 0x00000000);
        h1 = createHistoryData(1, 0xffffffff, 0xffffffff);
        assertTrue(h0.compareTo(h1) < 0);
    }

    @Test
    public void testCompareToGreat() {
        HistoryData h0;
        HistoryData h1;
        h0 = createHistoryData(1, 0x01234567, 0x01234567);
        h1 = createHistoryData(1, 0x00000000, 0x00000000);
        assertTrue(h0.compareTo(h1) > 0);

        h0 = createHistoryData(1, 0x7fffffff, 0xffffffff);
        h1 = createHistoryData(1, 0x00000000, 0x00000000);
        assertTrue(h0.compareTo(h1) > 0);

        h0 = createHistoryData(1, 0x7fffffff, 0x00000001);
        h1 = createHistoryData(1, 0x7fffffff, 0x00000000);
        assertTrue(h0.compareTo(h1) > 0);

        h0 = createHistoryData(1, 0x7fffffff, 0x80000000);
        h1 = createHistoryData(1, 0x7fffffff, 0x7fffffff);
        assertTrue(h0.compareTo(h1) > 0);

        h0 = createHistoryData(1, 0x80000000, 0x80000000);
        h1 = createHistoryData(1, 0x00000000, 0x00000000);
        assertTrue(h0.compareTo(h1) > 0);

        h0 = createHistoryData(1, 0xffffffff, 0xffffffff);
        h1 = createHistoryData(1, 0x00000000, 0x00000000);
        assertTrue(h0.compareTo(h1) > 0);

        h0 = createHistoryData(1, 0x80000000, 0x00000000);
        h1 = createHistoryData(1, 0x7fffffff, 0xffffffff);
        assertTrue(h0.compareTo(h1) > 0);

        h0 = createHistoryData(1, 0xffffffff, 0xffffffff);
        h1 = createHistoryData(1, 0x80000000, 0x00000000);
        assertTrue(h0.compareTo(h1) > 0);
    }

    @Test
    public void testCompareToId() {
        HistoryData h0;
        HistoryData h1;
        h0 = createHistoryData(1, 0x12345678, 0x12345678);
        h1 = createHistoryData(1, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) == 0);

        h0 = createHistoryData(0x123456789abcdef0L, 0x12345678, 0x12345678);
        h1 = createHistoryData(0x123456789abcdef0L, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) == 0);

        // less
        h0 = createHistoryData(0x0000000000000000L, 0x12345678, 0x12345678);
        h1 = createHistoryData(0x7fffffffffffffffL, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) < 0);

        h0 = createHistoryData(0x0000000000000000L, 0x12345678, 0x12345678);
        h1 = createHistoryData(0x8000000000000000L, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) < 0);

        h0 = createHistoryData(0x0000000000000000L, 0x12345678, 0x12345678);
        h1 = createHistoryData(0xffffffffffffffffL, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) < 0);

        h0 = createHistoryData(0x7fffffffffffffffL, 0x12345678, 0x12345678);
        h1 = createHistoryData(0x8000000000000000L, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) < 0);

        h0 = createHistoryData(0x7fffffffffffffffL, 0x12345678, 0x12345678);
        h1 = createHistoryData(0xffffffffffffffffL, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) < 0);

        h0 = createHistoryData(0x8000000000000000L, 0x12345678, 0x12345678);
        h1 = createHistoryData(0xffffffffffffffffL, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) < 0);

        // greater
        h0 = createHistoryData(0x7fffffffffffffffL, 0x12345678, 0x12345678);
        h1 = createHistoryData(0x0000000000000000L, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) > 0);

        h0 = createHistoryData(0x8000000000000000L, 0x12345678, 0x12345678);
        h1 = createHistoryData(0x0000000000000000L, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) > 0);

        h0 = createHistoryData(0xffffffffffffffffL, 0x12345678, 0x12345678);
        h1 = createHistoryData(0x0000000000000000L, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) > 0);

        h0 = createHistoryData(0x8000000000000000L, 0x12345678, 0x12345678);
        h1 = createHistoryData(0x7fffffffffffffffL, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) > 0);

        h0 = createHistoryData(0xffffffffffffffffL, 0x12345678, 0x12345678);
        h1 = createHistoryData(0x7fffffffffffffffL, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) > 0);

        h0 = createHistoryData(0xffffffffffffffffL, 0x12345678, 0x12345678);
        h1 = createHistoryData(0x8000000000000000L, 0x12345678, 0x12345678);
        assertTrue(h0.compareTo(h1) > 0);
    }

    /* -----------------------------------------------------------------------
     * Private methods
     * -------------------------------------------------------------------- */
    private HistoryData createHistoryData(long id, int sec, int ns) {
        HistoryData history = new HistoryData();
        history.id = id;
        history.sec = sec;
        history.ns = ns;
        return history;
    }
}
