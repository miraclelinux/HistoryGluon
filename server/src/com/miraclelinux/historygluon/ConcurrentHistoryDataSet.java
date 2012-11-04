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
