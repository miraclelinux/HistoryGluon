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
