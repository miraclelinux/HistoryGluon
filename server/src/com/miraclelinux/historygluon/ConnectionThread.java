package com.miraclelinux.historygluon;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConnectionThread extends Thread {
    /* -----------------------------------------------------------------------
     * Private members
     * -------------------------------------------------------------------- */
    private int m_port = 0;
    private Log m_log = null;
    private StorageDriver m_driver = null;

    /* -----------------------------------------------------------------------
     * Public Methods
     * -------------------------------------------------------------------- */
    public ConnectionThread(int port, StorageDriver driver) {
        m_port = port;
        m_driver = driver;
        m_log = LogFactory.getLog(ConnectionThread.class); 
    }

    @Override
    public void run() {
        ServerSocket server = null;
        try {
            server = new ServerSocket(m_port);
            m_log.info("start listening on port: " + server.getLocalPort());
            while (true) {
                Socket client = server.accept();
                StorageDriver driver = m_driver.createInstance();
                BridgeWorker bridge = new BridgeWorker(client, driver);
                bridge.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
            m_log.error(e);
        }
    }
}
