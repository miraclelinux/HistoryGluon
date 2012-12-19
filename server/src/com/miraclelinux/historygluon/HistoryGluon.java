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

import java.lang.reflect.Constructor;
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
    static private String m_storageName = null;
    static private String[] m_storageDriverArgs = null;
    static private boolean m_isDeleteDBMode = false;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public static void main(String[] args) throws Exception {
        m_log = LogFactory.getLog(HistoryGluon.class); 
        m_log.info("HistoryGluon, MIRACLE LINUX Corp. 2012");

        if (!parseArgs(args))
            return;

        StorageDriver driver = null;
        String prefix = "com.miraclelinux.historygluon.";
        String driverName = prefix + m_storageName + "Driver";
        try {
            Class<?> c = Class.forName(driverName);
            Class[] argTypes = { String[].class };
            Constructor constructor = c.getConstructor(argTypes);
            Object[] driverArgs = { m_storageDriverArgs };
            driver = (StorageDriver)constructor.newInstance(driverArgs);
        } catch (ClassNotFoundException e) {
            m_log.error("Unknown Storage Type: " + m_storageName);
            printUsage();
            return;
        } catch (Exception e) {
            throw e;
        }

        if (!driver.init()) {
            m_log.fatal("Failed to initialize Storage Driver.");
            return;
        }
        m_log.info("StorageDriver: " + driver.getName());

        if (m_isDeleteDBMode) {
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

    public static boolean hasStorageDriver(String storageName) {
        String prefix = "com.miraclelinux.historygluon.";
        String driverName = prefix + storageName + "Driver";
        try {
            Class<?> c = Class.forName(driverName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /* -----------------------------------------------------------------------
     * Private Methods
     * -------------------------------------------------------------------- */
    private static boolean parseArgs(String[] args) {
        if (args.length < 1) {
            printUsage();
            return false;
        }

        m_storageName = args[0];

        int nGlobalArgs = 1;

        if (args.length >= 2 && args[1].equals("--delete-db")) {
            m_isDeleteDBMode = true;
            nGlobalArgs++;
        }

        m_storageDriverArgs = new String[args.length - nGlobalArgs];
        for (int i = nGlobalArgs; i < args.length; i++) {
            m_storageDriverArgs[i - nGlobalArgs] = args[i];
        }

        return true;
    }

    private static void printUsage() {
        String[] drivers = {"HBase", "Cassandra", "Riak", "Mem"};
        System.out.println("Usage:");
        System.out.println(" HistoryServer StorageName [--delete-db]");
        System.out.println("");
        System.out.println(" StorageName: One of the following.");
        for (String driver : drivers) {
            if (hasStorageDriver(driver))
                System.out.println("   " + driver);
        }
        System.out.println("");
        System.out.println(" --delete-db: Delete DataBase and quit");
        System.out.println("");
    }
}
