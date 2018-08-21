package org.verdictdb;

import org.verdictdb.exception.VerdictDBException;
import py4j.GatewayServer;

public class VerdictGateway {
    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            VerdictContext v = VerdictContext.fromConnectionString("jdbc:mysql://localhost:3306/instacart", "root", "");
            GatewayServer gatewayServer = new GatewayServer(v);
            gatewayServer.start();
            System.out.println("Gateway Server Started");
        }
        catch (VerdictDBException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }

    }
}
