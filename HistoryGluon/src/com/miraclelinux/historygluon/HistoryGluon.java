package com.miraclelinux.historygluon;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HistoryGluon {
    /* -----------------------------------------------------------------------
     * Const Members
     * -------------------------------------------------------------------- */
    private static final int DEFAULT_PORT = 30010;

    /* -----------------------------------------------------------------------
     * Private Members
     * -------------------------------------------------------------------- */
    static private Log m_log = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public static void main(String[] args) {
        m_log = LogFactory.getLog(HistoryGluon.class); 
        m_log.info("HistoryGluon, MIRACLE LINUX Corp. 2012");
        // check Storage Type
        if (args.length < 1) {
            printUsage();
            return;
        }

        StorageDriver driver = null;
        String storageName = args[0];
        if (storageName.equals("HBase"))
            driver = new HBaseDriver();
        else if (storageName.equals("Cassandra"))
            driver = new CassandraDriver();
        else if (storageName.equals("Riak"))
            driver = new RiakDriver();
        else if (storageName.equals("Mem"))
            driver = new MemDriver();
        else {
            m_log.error("Unknown Storage Type: " + storageName);
            printUsage();
            return;
        }

        if (!driver.init()) {
            m_log.fatal("Failed to initialize Storage Driver.");
            return;
        }
        m_log.info("StorageDriver: " + driver.getName());

        // check if --delete-db is specfied
        if (args.length >= 2 && args[1].equals("--delete-db")) {
            m_log.info("try to deleted DB...");
            if (driver.deleteDB())
                m_log.info("Success: Deleted DB");
            else
                m_log.info("Failed: Deleted DB");
            driver.close();
            return;
        }

        ConnectionThread connectionThread = new ConnectionThread(DEFAULT_PORT,
                                                                 driver);
        connectionThread.start();
        driver.close();
    }

    /* -----------------------------------------------------------------------
     * Private Methods
     * -------------------------------------------------------------------- */
    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println(" HistoryServer StorageName [--delete-db]");
        System.out.println("");
        System.out.println(" StorageName: One of the following.");
        System.out.println("   HBase");
        System.out.println("   Cassandra");
        System.out.println("   Riak");
        System.out.println("   Mem");
        System.out.println("");
        System.out.println(" --delete-db: Delete DataBase and quit");
        System.out.println("");
    }
}
