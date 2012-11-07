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

public class ErrorCode {
    public static final int SUCCESS                      = 0;
    public static final int UNKNOWN_ERROR                = 1;
    public static final int SERVER_ERROR                 = 2;
    public static final int UNSUPPORTED_PROTOCOL_VERSION = 3;
    public static final int NOT_IMPLEMENTED              = 4;
    public static final int AUTHENTIFICATION_FAILED      = 100;
    public static final int PACKET_SHORT                 = 200;
    public static final int INVALID_DATA_TYPE            = 201;
    public static final int INVALID_SORT_ORDER           = 202;
    public static final int ENTRY_EXISTS                 = 300;
    public static final int NOT_FOUND                    = 301;
    public static final int TOO_MANY_ENTRIES             = 302;

    public static final int HBASE_EXCEPTION              = 10000;
    public static final int CASSANDRA_EXCEPTION          = 20000;
    public static final int RIAK_EXCEPTION               = 30000;

    // The following errors are internally used.
    public static final int IERR_READ_STREAM_END         = -1;
}
