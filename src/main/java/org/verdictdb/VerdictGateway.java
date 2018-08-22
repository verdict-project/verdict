package org.verdictdb;

import org.verdictdb.coordinator.VerdictSingleResult;
import org.verdictdb.exception.VerdictDBException;
import py4j.GatewayServer;

public class VerdictGateway {
    public static void main(String[] args) {
        try {
//            VerdictContext v = VerdictContext.fromConnectionString("jdbc:mysql://localhost:3306/instacart", "root", "");
//            VerdictSingleResult r = v.sql("select count(1) from instacart.orders_joined");
//            r.next();
//            System.out.println(r.getInt(0));
            GatewayServer gatewayServer = new GatewayServer(new VerdictContext());
            gatewayServer.start();
            System.out.println("Gateway Server Started");
        }
        catch (VerdictDBException e){
            e.printStackTrace();
        }

    }
}
