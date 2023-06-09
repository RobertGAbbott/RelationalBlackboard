/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;


import org.junit.Test;
import static org.junit.Assert.*;

import gov.sandia.rbb.*;
import java.sql.SQLException;
import java.util.Map;

/**
 *
 * @author rgabbot
 */
public class H2SStringTest
{


    @Test
    public void testStrings() throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        assertNull(H2SString.find(rbb.db(), "whatever"));

        final Long id1 = H2SString.toID(rbb.db(), "test");
        final Long id2 = H2SString.toID(rbb.db(), "test2");
        final Long id3 = H2SString.toID(rbb.db(), "test");
        final Long id4 = H2SString.find(rbb.db(), "test2");
        final Long id5 = H2SString.toID(rbb.db(), "tes't2");

        assertEquals("test", H2SString.fromID(rbb.db(), id3));
        assertEquals("tes't2", H2SString.fromID(rbb.db(), id5));

        assertEquals(id1, id3);
        assertEquals(id2, id4);

        rbb.disconnect();
    }

    @Test
    public void testNullString() throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        Long id = H2SString.toID(rbb.db(), null);

        assertNull(H2SString.fromID(rbb.db(), id));

        rbb.disconnect();
    }

    @Test
    public void testFindMulti() throws SQLException
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        Long a = H2SString.toID(rbb.db(), "a");
        Long b = H2SString.toID(rbb.db(), "b");

        Map<String, Long> result = H2SString.findSet(rbb.db(), new String[]{"a", "b", "c", null, "d"});

        assertEquals(2, result.size()); // just a and b
        assertEquals(a, result.get("a"));
        assertEquals(b, result.get("b"));


        rbb.disconnect();
    }

    @Test
    public void testFindArray() throws SQLException
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        Long a = H2SString.toID(rbb.db(), "a");
        Long b = H2SString.toID(rbb.db(), "b");

//        H2SString.findArray(rbb.db(), new String[]{"a", "b", "c", null, "d"});
//
//        assertEquals(2, result.size()); // just a and b
//        assertEquals(a, result.get("a"));
//        assertEquals(b, result.get("b"));


          assertArrayEquals(new Object[]{ a , new Object[]{ a , b ,new Object[]{ b , 0L}}},
          H2SString.findArray(rbb.db(),
                            new Object[]{"a", new Object[]{"a","b",new Object[]{"b", null}}}));

        rbb.disconnect();
    }


    @Test
    public void testFromIDs() throws SQLException
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        String[] names = {"zero", "one", "two" };
        Long[] ids = new Long[names.length];
        for(int i = 0; i < names.length; ++i)
            ids[i] = H2SString.toID(rbb.db(), names[i]);

        String[] a = H2SString.fromIDs(rbb.db(), new Long[]{ids[1], ids[1], 666L, ids[2], null});
        assertEquals(a.length, 5); // one result per entry in query, even if there are duplicates.
        assertEquals(a[0], "one");
        assertEquals(a[1], "one"); // duplicates handled correctly.
        assertEquals(a[2], null); // invalid id (666L) results in null
        assertEquals(a[3], "two");
        assertEquals(a[4], null); // invalid id (null) results in null



        rbb.disconnect();
    }
}
