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
