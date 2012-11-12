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

import java.util.HashMap;
import java.util.Comparator;
import java.util.Comparator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MemDriver extends BasicStorageDriver {

    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private static ReadWriteLock m_rwlock = new ReentrantReadWriteLock();
    private static HashMap<String,ConcurrentHistoryDataSet> m_dataSetMap
      = new HashMap<String,ConcurrentHistoryDataSet>();
    private Log m_log = null;
    private String m_dbName = null;
    private ConcurrentHistoryDataSet m_dataSetPreferId = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public MemDriver() {
        m_log = LogFactory.getLog(MemDriver.class); 
    }

    @Override
    public boolean init() {
        return true;
    }

    @Override
    public StorageDriver createInstance() {
        StorageDriver driver = new MemDriver();
        driver.init();
        return driver;
    }

    @Override
    public String getName() {
        return "Mem";
    }

    @Override
    public void setDatabase(String dbName) {
        m_dbName = dbName;

        // check if there is the ConcurrentHistoryDataSet instance with dbName
        ConcurrentHistoryDataSet set = null;
        try {
            m_rwlock.readLock().lock();
            set = m_dataSetMap.get(m_dbName);
        } finally {
            m_rwlock.readLock().unlock();
        }
        if (set != null) {
            m_dataSetPreferId = set;
            return;
        }

        // make a ConcurrentHistoryDataSet instance
        Comparator<HistoryData> comparator;
        comparator = new HistoryDataComparatorPreferId();
        ConcurrentHistoryDataSet newSet =
          new ConcurrentHistoryDataSet(comparator);

        // add the instance with dbName into the map
        try {
            m_rwlock.writeLock().lock();
            // other thread might create the set after the above checa.
            // So we have to check it again.
            set = m_dataSetMap.get(m_dbName);
            if (set == null) {
                m_dataSetMap.put(m_dbName, newSet);
                set = newSet;
            }
        } finally {
            m_rwlock.writeLock().unlock();
        }
        m_dataSetPreferId = set;
    }

    @Override
    public int addData(HistoryData history) {
        if (!m_dataSetPreferId.add(history))
            return ErrorCode.ENTRY_EXISTS;
        return ErrorCode.SUCCESS;
    }

    @Override
    public boolean deleteDB() {
        // Nothing to do, because data is lost when the server terminates.
        return true;
    }

    /* -----------------------------------------------------------------------
     * Protected Methods
     * -------------------------------------------------------------------- */
    @Override
    protected HistoryDataSet
      getDataSet(long itemId, String startKey, String stopKey, int maxCount) {
        HistoryData historyForComp0 = new HistoryData();
        HistoryData historyForComp1 = new HistoryData();
        fillItemIdClockNsWithKey(startKey, historyForComp0);
        fillItemIdClockNsWithKey(stopKey, historyForComp1);
        return m_dataSetPreferId.createSubSet(historyForComp0, historyForComp1);
    }

    @Override
    protected boolean deleteRow(HistoryData history, Object arg) {
        return m_dataSetPreferId.delete(history);
    }
}
