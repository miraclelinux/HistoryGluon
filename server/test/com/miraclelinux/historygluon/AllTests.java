package com.miraclelinux.historygluon;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({UtilsTest.class, HistoryDataTest.class, BasicStorageDriverTest.class})
public class AllTests {
}
