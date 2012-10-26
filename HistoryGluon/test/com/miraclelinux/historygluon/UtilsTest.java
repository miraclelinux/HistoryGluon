package com.miraclelinux.historygluon;

import org.junit.Test;
import static org.junit.Assert.*;

public class UtilsTest
{
    @Test
    public void testHexCharToIntNumeric() {
        assertEquals(0, Utils.hexCharToInt('0'));
        assertEquals(1, Utils.hexCharToInt('1'));
        assertEquals(2, Utils.hexCharToInt('2'));
        assertEquals(3, Utils.hexCharToInt('3'));
        assertEquals(4, Utils.hexCharToInt('4'));
        assertEquals(5, Utils.hexCharToInt('5'));
        assertEquals(6, Utils.hexCharToInt('6'));
        assertEquals(7, Utils.hexCharToInt('7'));
        assertEquals(8, Utils.hexCharToInt('8'));
        assertEquals(9, Utils.hexCharToInt('9'));
    }

    @Test
    public void testHexCharToIntHexAlphabetLower() {
        assertEquals(10, Utils.hexCharToInt('a'));
        assertEquals(11, Utils.hexCharToInt('b'));
        assertEquals(12, Utils.hexCharToInt('c'));
        assertEquals(13, Utils.hexCharToInt('d'));
        assertEquals(14, Utils.hexCharToInt('e'));
        assertEquals(15, Utils.hexCharToInt('f'));
    }

    @Test
    public void testHexCharToIntHexAlphabetUpper() {
        assertEquals(10, Utils.hexCharToInt('A'));
        assertEquals(11, Utils.hexCharToInt('B'));
        assertEquals(12, Utils.hexCharToInt('C'));
        assertEquals(13, Utils.hexCharToInt('D'));
        assertEquals(14, Utils.hexCharToInt('E'));
        assertEquals(15, Utils.hexCharToInt('F'));
    }
}
