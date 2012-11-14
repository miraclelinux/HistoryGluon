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

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HistoryStreamer extends Thread {

    /* -----------------------------------------------------------------------
     * Private constants
     * -------------------------------------------------------------------- */
    private static final int THREAD_QUEUE_CAPACITY = 10;
    private static final int NUM_MAX_DATA_AT_ONCE = 100;

    /* -----------------------------------------------------------------------
     * Private static members
     * -------------------------------------------------------------------- */
    private static ArrayBlockingQueue<HistoryStreamer> m_threadQueue = null;

    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private Log m_log = null;
    private BasicStorageDriver m_basicDriver = null;
    private Semaphore m_semaphore = null;
    private Semaphore m_working_sem = null;
    private AtomicBoolean m_stopRequested = new AtomicBoolean(false);
    private BlockingQueue<HistoryStreamElement> m_queue = null;
    private String m_key0 = null;
    private String m_key1 = null;

    static {
        // We make a list of HistoryStreamer first, because we want to avoid to
        // add try block for BLockingQueue#put() here.
        List<HistoryStreamer> streamers
          = new ArrayList<HistoryStreamer>(THREAD_QUEUE_CAPACITY);
        for (int i = 0; i < THREAD_QUEUE_CAPACITY; i++) {
            HistoryStreamer thread = new HistoryStreamer();
            thread.start();
            streamers.add(thread);
        }
        m_threadQueue = new ArrayBlockingQueue<HistoryStreamer>
                              (THREAD_QUEUE_CAPACITY, true, streamers);
    }

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public static HistoryStreamer getInstance(BasicStorageDriver basicDriver) {
        HistoryStreamer thread = null;
        try {
            thread = m_threadQueue.take();
            thread.m_basicDriver = basicDriver;
        } catch (InterruptedException e) {
            /* nothing to do */
        }
        return thread;
    }

    public void close() {
        if (!m_working_sem.tryAcquire()) {
            // This condition happens when the thread is now runinng
            // in generateStream(). We have to wait for stopping it.
            // [TODO] However, It's better to avoid bloking here.
            // This function should return soon.
            m_stopRequested.set(true);
            try {
                m_working_sem.acquire();
            } catch (InterruptedException e) {
                // TODO: What should we do here ?
            }
            m_stopRequested.set(false);
        }
        m_working_sem.release();

        // clear the stream queue
        m_queue.clear();

        // return this instance to the thread pool.
        m_threadQueue.add(this);
    }

    public BlockingQueue<HistoryStreamElement> open(String key0, String key1) {
        if (!m_working_sem.tryAcquire()) {
            String msg;
            msg = "HistoryStreamer#open(): Stream has already been opened.";
            throw new InternalCheckException(msg);
        }
        m_working_sem.release();

        if (!m_queue.isEmpty()) {
            String msg;
            msg = "HistoryStreamer#open(): queue is not empty.";
            throw new InternalCheckException(msg);
        }

        m_key0 = key0;
        m_key1 = key1;
        m_semaphore.release();
        return m_queue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                m_semaphore.acquire();
                m_working_sem.acquire();
                generateStream();
            } catch (Exception e) {
                e.printStackTrace();
                m_log.error(e);
                putEndOfStream(ErrorCode.UNKNOWN_ERROR);
            } finally {
                m_working_sem.release();
            }
        }
    }

    /* -----------------------------------------------------------------------
     * Private Methods
     * -------------------------------------------------------------------- */
    private HistoryStreamer() {
        m_log = LogFactory.getLog(HistoryStreamer.class); 
        m_queue = new LinkedBlockingQueue<HistoryStreamElement>();
        m_semaphore = new Semaphore(1);
        m_semaphore.acquireUninterruptibly(); // This is never blocked.
        m_working_sem = new Semaphore(1);
    }

    private void generateStream() throws InterruptedException {
        while (!m_stopRequested.get()) {
            HistoryDataSet dataSet;
            try {
                dataSet = m_basicDriver.getDataSet(m_key0, m_key1,
                                                   NUM_MAX_DATA_AT_ONCE);
            } catch (HistoryDataSet.TooManyException e) {
                // This won't be occured, because NUM_MAX_DATA_AT_ONCE is
                // not so large.
                break;
            }

            int dataSize = dataSet.size();
            if (dataSize == 0)
                break;

            // push obtained data to the queue
            Iterator<HistoryData> it = dataSet.iterator();
            HistoryData history = null;
            while (it.hasNext()) {
                history = it.next();
                m_queue.put(new HistoryStreamElement(history));
            }

            // break if this is the last chunk.
            if (dataSize <  NUM_MAX_DATA_AT_ONCE)
                break;

            if (history.sec == 0xffffffff && history.ns == 0xffffffff)
                m_key0 =  m_basicDriver.makeKey(history.id+1, 0, 0);
            else
                m_key0 =  m_basicDriver.makeKey(history.id, history.sec,
                                                history.ns + 1);
        }
        putEndOfStream(ErrorCode.SUCCESS);
    }

    private void putEndOfStream(int errorCode) {
        HistoryStreamElement eos
          = HistoryStreamElement.createEndOfStream(errorCode);
        try {
            m_queue.put(eos);
        } catch (InterruptedException e) {
            // TODO: implement exception
            e.printStackTrace();
            m_log.error(e);
        }
    }
}
