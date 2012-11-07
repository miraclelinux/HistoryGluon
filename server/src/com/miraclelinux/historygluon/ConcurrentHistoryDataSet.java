/* History Gluon
   Copyright (C) 2012 MIRACLE LINUX CORPRATION
 
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

import java.util.Comparator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentHistoryDataSet {
    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private ReadWriteLock m_rwlock = new ReentrantReadWriteLock();
    private HistoryDataSet m_dataSet = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public ConcurrentHistoryDataSet(Comparator<HistoryData> comparator) {
        m_dataSet = new HistoryDataSet(comparator);
    }

    public boolean add(HistoryData history) {
        boolean ret = false;
        try {
            m_rwlock.writeLock().lock();
            ret = m_dataSet.add(history);
            if (!ret)
                ret = replaceElement(history);
        } finally {
            m_rwlock.writeLock().unlock();
        }
        return ret;
    }

    public boolean delete(HistoryData history) {
        boolean ret = false;

        HistoryData entry = get(history);
        if (entry == null)
            return false;

        try {
            m_rwlock.writeLock().lock();
            ret = m_dataSet.remove(entry);
        } finally {
            m_rwlock.writeLock().unlock();
        }
        return ret;
    }

    public HistoryData get(HistoryData keyHistory) {
        HistoryData history = null;
        try {
            m_rwlock.readLock().lock();
            history = m_dataSet.ceiling(keyHistory);
            if (history != null) {
                if (history.compareTo(keyHistory) != 0)
                    history = null;
            }
        } finally {
            m_rwlock.readLock().unlock();
        }
        return history;
    }

    public HistoryDataSet createSubSet(HistoryData history0,
                                       HistoryData history1) {
        HistoryDataSet dataSet = new HistoryDataSet();
        try {
            m_rwlock.readLock().lock();
            for (HistoryData history : m_dataSet.subSet(history0, history1))
                dataSet.add(new HistoryData(history));
        } finally {
            m_rwlock.readLock().unlock();
        }
        return dataSet;
    }

    /* -----------------------------------------------------------------------
     * Private Methods
     * -------------------------------------------------------------------- */
    private boolean replaceElement(HistoryData history) {
        delete(history);
        return m_dataSet.add(history);
    }

}
