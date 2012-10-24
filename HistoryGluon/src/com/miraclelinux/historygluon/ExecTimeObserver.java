package com.miraclelinux.historygluon;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExecTimeObserver {

    private long m_t0;
    private long m_t1;
    private boolean m_enabled = false;
    private String m_logHeader = null;
    private Log m_log = null;
    private long m_threadId = 0;

    public ExecTimeObserver(String header) {
        m_log = LogFactory.getLog(ExecTimeObserver.class);
        m_threadId = Thread.currentThread().getId();
        m_logHeader = header + "(" + m_threadId + ") : ";
    }

    public void setEnable() {
        m_enabled = true;
    }

    public void setDisable() {
        m_enabled = false;
    }

    public void start() {
        if (!m_enabled)
            return;
        m_t0 = System.currentTimeMillis();
    }

    public void stopAndLog(String comment) {
        if (!m_enabled)
            return;
        m_t1 = System.currentTimeMillis();
        long dt = m_t1 - m_t0;
        m_log.info(m_logHeader + comment + " : " + dt + " [ms]" );
    }
};
