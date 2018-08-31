package org.verdictdb;

import org.verdictdb.commons.VerdictDBLogger;

import py4j.GatewayServer;

public class VerdictGateway {

    private static VerdictDBLogger logger = VerdictDBLogger.getLogger(VerdictGateway.class);

    public static void main(String[] args) {
        GatewayServer gatewayServer = new GatewayServer();
        gatewayServer.start();
        logger.debug("Gateway Server Started");
    }
}
