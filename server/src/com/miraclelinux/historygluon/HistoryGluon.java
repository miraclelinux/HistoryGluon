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
