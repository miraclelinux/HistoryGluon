package com.miraclelinux.historygluon;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentHistoryDataSet extends HistoryDataSet {
    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private ReadWriteLock m_rwlock = new ReentrantReadWriteLock();

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    @Override
    public boolean add(HistoryData history) {
        boolean ret = false;
        try {
            m_rwlock.writeLock().lock();
            ret = super.add(history);
        } finally {
            m_rwlock.writeLock().unlock();
        }
        return ret;
    }

    public boolean remove(HistoryData history) {
        boolean ret = false;
        try {
            m_rwlock.writeLock().lock();
            ret = super.remove(history);
        } finally {
            m_rwlock.writeLock().unlock();
        }
        return ret;
    }

    public HistoryData get(HistoryData keyHistory) {
        HistoryData history = null;
        try {
            m_rwlock.readLock().lock();
            history = ceiling(history);
            if (history != null) {
                if (history.compareTo(keyHistory) != 0)
                    history = null;
            }
        } finally {
            m_rwlock.readLock().unlock();
        }
        return history;
    }

    public HistoryDataSet createSubSet(HistoryData history0, HistoryData history1) {
        HistoryDataSet dataSet = new HistoryDataSet();
        try {
            m_rwlock.readLock().lock();
            for (HistoryData history : subSet(history0, history1))
                dataSet.add(new HistoryData(history));
        } finally {
            m_rwlock.readLock().unlock();
        }
        return dataSet;
    }
}
