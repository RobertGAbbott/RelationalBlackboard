
package gov.sandia.rbb.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class SQLRBBExample
{

    public static void main(String[] args)
        throws Exception
    {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(
            "jdbc:h2:/Users/klakkar/Projects/CapableManpower/RBBPortToCVS/RBB-08232010/RBB/test/db_for_example_like_edrt/db_for_example_like_edrt",
            "sa", "x");
        System.out.println("Connection created: " + conn);
        ResultSet seq_infos =
            conn.createStatement().executeQuery(
            "call rbb_find_sequences('attribute=pos,domain=air,side=blue',null,null,null)");
        while (seq_infos.next())
        {
            ResultSet tags = conn.createStatement().executeQuery("call rbb_get_all_tags(" + seq_infos.getLong(
                1) + ")");
            if (tags.next())
            {
                System.out.println(tags.getString(1));
            }
            ResultSet pos = conn.createStatement().executeQuery("select TIME,X,Y from RBB_DATA2 where SEQ_ID = " + seq_infos.getLong(
                1));
            while (pos.next())
            {
                System.out.println("" + pos.getDouble(1) + "," + pos.getDouble(
                    2) + "," + pos.getDouble(3));
            }
            System.out.println("");
        }
        conn.close();
    }

}
