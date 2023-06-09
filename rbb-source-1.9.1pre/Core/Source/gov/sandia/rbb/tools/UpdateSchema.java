
package gov.sandia.rbb.tools;

import gov.sandia.rbb.impl.h2.statics.H2SRBB;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * The purpose of this class is to migrate an RBB from an older schema version
 * to the current schema version.
 * 
 * 
 * @author rgabbot
 */
public class UpdateSchema
{
    public static void main(String[] args) {
        final String usage = "UpdateSchema [RBB_URL]: update the specified RBB to current RBB schema.";
        
        if(args.length != 1) {
            System.err.println(usage);
            System.exit(1);
        }

        String url = args[0];

        // don't create a new db
        url += ";ifexists=true";
        
        try {
            // ensure driver is available.
            Class.forName("org.h2.Driver"); 
            
            // open db
            Connection db = java.sql.DriverManager.getConnection(url, "sa", "x");        

            // get previous schema version
            ResultSet rs = db.createStatement().executeQuery("select RBB_SCHEMA_VERSION from RBB_DESCRIPTOR");

            if(!rs.next())
                throw new SQLException("H2RBB.connect: the RBB_DESCRIPTOR table exists but is empty.  That's not supposed to happen!");

            int oldVersion = rs.getInt(1);
            if(oldVersion == H2SRBB.schemaVersion()) {
                System.err.println("The RBB is already at schema version " + oldVersion + ", which matches this version of RBB.  No changes made.");
                System.exit(1);
            }
            
            System.err.println("The RBB was at schema version " + oldVersion);

            while(oldVersion < H2SRBB.schemaVersion()) {
                if(oldVersion == 4) {
                    String q = "";
                    q += "CREATE ALIAS RBB_NEGATIVE for \"gov.sandia.rbb.impl.h2.statics.H2SRBB.negative\";";
                    q += "CREATE ALIAS RBB_GET_SERVER_ADDRESS for \"gov.sandia.rbb.impl.h2.statics.H2SRBB.privateGetServerAddress\";";
                    q += "CREATE ALIAS RBB_PRIVATE_SET_LOCAL for \"gov.sandia.rbb.impl.h2.statics.H2SRBB.privateSetLocal\";";
                    db.createStatement().execute(q);
                    oldVersion=5;
                }
                else {
                    System.err.println("Don't know how to upgrade from schema version "+oldVersion);
                    System.exit(1);
                }
                
                db.createStatement().execute("UPDATE RBB_DESCRIPTOR set RBB_SCHEMA_VERSION = "+oldVersion);
                System.err.println("Upgraded to version "+oldVersion);
            }
        } catch (Exception e) {
            System.err.println("ERROR: " + e.toString());
            System.exit(1);
        }
        
    }
}
