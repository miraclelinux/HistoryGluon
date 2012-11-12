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

import java.util.TreeSet;
import java.util.Comparator;

public class HistoryDataSet extends TreeSet<HistoryData> {

    // -----------------------------------------------------------------------
    // Public inner class
    // -----------------------------------------------------------------------
    public static class TooManyException extends Exception {
    }

    public static class EmptyException extends Exception {
    }

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public HistoryDataSet() {
    }

    public HistoryDataSet(Comparator<HistoryData> comparator) {
        super(comparator);
    }
}
