package org.verdictdb;

import org.verdictdb.VerdictContext;
import py4j.GatewayServer;

public class VerdictContextConnector {
    public static void main(String[] args) {
        GatewayServer gatewayServer = new GatewayServer(new VerdictContext());
        gatewayServer.start();
        System.out.println("Gateway Server Started");
    }
}
