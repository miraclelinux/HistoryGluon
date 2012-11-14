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

public class HistoryStreamElement {
    // -----------------------------------------------------------------------
    // Private member
    // -----------------------------------------------------------------------
    private boolean m_endOfStream = false;
    private HistoryData m_history = null;
    private int m_errorCode = ErrorCode.UNKNOWN_ERROR;

    // -----------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------- */
    public HistoryStreamElement(HistoryData history) {
        m_history = history;
    }

    public static HistoryStreamElement createEndOfStream(int errorCode) {
        HistoryStreamElement elem = new HistoryStreamElement(null);
        elem.m_endOfStream = true;
        elem.m_errorCode = errorCode;
        return elem;
    }

    public boolean isEndOfStream() {
        return m_endOfStream;
    }

    public HistoryData getData() {
        return m_history;
    }

    public int getErrorCode() {
        return m_errorCode;
    }
}

