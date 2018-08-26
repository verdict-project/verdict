package org.verdictdb;

import py4j.GatewayServer;
import org.verdictdb.commons.VerdictDBLogger;

public class VerdictGateway {

    private static VerdictDBLogger logger = VerdictDBLogger.getLogger(VerdictGateway.class);

    public static void main(String[] args) {
        GatewayServer gatewayServer = new GatewayServer();
        gatewayServer.start();
        logger.debug("Gateway Server Started");
    }
}
