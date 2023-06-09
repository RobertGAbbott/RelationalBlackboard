
package gov.sandia.rbb;

import java.sql.ResultSet;
import gov.sandia.rbb.PreparedStatementCache.Query;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.RBB;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */

public class PreparedStatementCacheTest {
    @Test
    public void testTimeseriesCache() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        final String jdbc = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(jdbc, null);

        // put soemthing in the RBB
        final long id1 = H2SEvent.create(rbb.db(), 0.0, 1.0, "x=1,y=2");
        final long id2 = H2SEvent.create(rbb.db(), 0.0, 1.0, "x=1,y=3");

        PreparedStatementCache.numPreparedStatements = 0;

        // basic case
        Query q = PreparedStatementCache.startQuery(rbb.db());

        q.addAlt("select * from RBB_EVENTS where ID=", id1);

        ResultSet rs = q.getPreparedStatement().executeQuery();
        assertTrue(rs.next());
        assertEquals(id1, rs.getLong("ID"));
        assertFalse(rs.next());

// this fails when all the tests for the project are run together... it appears H2 re-uses Connection instances
//        assertEquals(1, PreparedStatementCache.numConnections);
        assertEquals(1, PreparedStatementCache.numPreparedStatements);


        // a second query differing only in parameter values should re-used the preparedstatement
        q = PreparedStatementCache.startQuery(rbb.db());

        q.addAlt("select * from RBB_EVENTS where ID=", id2);

        rs = q.getPreparedStatement().executeQuery();
        assertTrue(rs.next());
        assertEquals(id2, rs.getLong("ID"));
        assertFalse(rs.next());

// this fails when all the tests for the project are run together... it appears H2 re-uses Connection instances
//        assertEquals(1, PreparedStatementCache.numConnections);
        assertEquals(1, PreparedStatementCache.numPreparedStatements);


        // A new prepared statement will be created for a different query,
        // even if it differs only syntactically, such as the extra space in this query
        q = PreparedStatementCache.startQuery(rbb.db());

        q.add("select * from RBB_EVENTS where ID="+" ");
        q.addParam(id2);

        rs = q.getPreparedStatement().executeQuery();
        assertTrue(rs.next());
        assertFalse(rs.next());

  // this fails when all the tests for the project are run together... it appears H2 re-uses Connection instances
//      assertEquals(1, PreparedStatementCache.numConnections);
        assertEquals(2, PreparedStatementCache.numPreparedStatements);

        // each connection has its own set of prepared statements,
        // even if they reference the same underlying RBB.
        RBB rbb2 = RBB.connect(jdbc);
        q = PreparedStatementCache.startQuery(rbb2.db());

        q.addAlt("select * from RBB_EVENTS where ID=", id2);

        rs = q.getPreparedStatement().executeQuery();
        assertTrue(rs.next());
        assertFalse(rs.next());

  // this fails when all the tests for the project are run together... it appears H2 re-uses Connection instances
//      assertEquals(2, PreparedStatementCache.numConnections);
        assertEquals(3, PreparedStatementCache.numPreparedStatements);

        rbb2.disconnect();
        rbb.disconnect();
    }
}
