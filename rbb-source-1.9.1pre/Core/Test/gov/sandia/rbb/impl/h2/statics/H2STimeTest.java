/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;

import org.junit.Test;
import static org.junit.Assert.*;
import gov.sandia.rbb.*;
import java.sql.ResultSet;
import static gov.sandia.rbb.impl.h2.statics.H2SRBBTest.*;
/**
 *
 * @author rgabbot
 */
public class H2STimeTest {

    @Test
    public void testTime()
        throws Exception
    {
        final String methodName =
            java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        java.sql.Statement s = rbb.db().createStatement();
        ResultSet rs = null;

        rs = rbb.db().createStatement().executeQuery(
                "call rbb_define_time_coordinate('timeCoordinate=millisecondsUTC', 1000, 0);");

        try {
            rs = s.executeQuery("call rbb_define_time_coordinate('timeCoordinate=millisecondsUTC,x=y', 1000, 0);");
            fail("The previous statement was supposed to fail because timeCoordinate millisecondsUTS was previous defined without being conditioned on x");
        } catch(java.sql.SQLException e) {
            // System.err.println("This exception was expected: " + e.toString());
        }

         rs = s.executeQuery("call rbb_define_time_coordinate('timeCoordinate=sessionSeconds,session=1', 1, -100);");
         rs = s.executeQuery("call rbb_define_time_coordinate('timeCoordinate=sessionSeconds,session=2', 1, -200);");


        // 100 seconds in is 100 * 1000 in milliseconds = 100000
        // also, the extra tags in the tagset are ignored.
        rs = s.executeQuery("call rbb_convert_time(100000, 'timeCoordinate=millisecondsUTC,a=b', 'x=y,timeCoordinate=sessionSeconds,session=1')");
        assert(rs.next());
        assertEquals(0, rs.getDouble(1), 1e-6);

        // 300,000 ms is 100 s after sessoin 2 started.
        rs = s.executeQuery("call rbb_convert_time(300000, 'timeCoordinate=millisecondsUTC,a=b', 'x=y,timeCoordinate=sessionSeconds,session=2')");
        assert(rs.next());
        assertEquals(100, rs.getDouble(1), 1e-6);

        // attempt with insufficient context tags.
        try {
            rs = s.executeQuery("call rbb_convert_time(100000, 'timeCoordinate=millisecondsUTC', 'timeCoordinate=sessionSeconds')");
            fail("The previous statement was supposed to fail because sessionSeconds is conditioned on session, which was not given.");
        } catch(Exception e) {
        }

        // now specify session in the first tagset - it should provide the context for the second time coordinate.
        rs = s.executeQuery("call rbb_convert_time(100000, 'timeCoordinate=millisecondsUTC,a=b,session=1,x=y', 'timeCoordinate=sessionSeconds')");
        assert(rs.next());
        assertEquals(0, rs.getDouble(1), 1e-6);

        // ...but values provided in each tagset should override values provided in the other
        // so here, provide session in both, even though only sessionSeconds needs it, and make sure session=2 takes precedence for sessionSeconds.
        rs = s.executeQuery("call rbb_convert_time(300000, 'timeCoordinate=millisecondsUTC,a=b,session=1,x=y', 'timeCoordinate=sessionSeconds,session=2')");
        assert(rs.next());
        assertEquals(100, rs.getDouble(1), 1e-6);

        rs = s.executeQuery("call rbb_delete_time_coordinates('timeCoordinate=sessionSeconds');");

        assertEquals("Only millisecondsUTC should remain", 1, countQueryResults(rbb.db(), "select * from RBB_TIME_COORDINATES"));

        rbb.disconnect();
    }
}
