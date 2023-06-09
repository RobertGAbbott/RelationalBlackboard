/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;

import gov.sandia.rbb.*;
import java.util.HashSet;
import java.util.Set;


/**
 *
 * @author rgabbot
 */
public class H2STagsetTest
{

    @Test
    public void testFindCombinations() throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        H2SEvent.create(rbb.db(), 1.0, 2.0, "x=1,y=1");
        H2SEvent.create(rbb.db(), 1.0, 2.0, "x=1,y=2");
        H2SEvent.create(rbb.db(), 1.0, 2.0, "x=1,y=2"); // repeat of previous
        H2SEvent.create(rbb.db(), 1.0, 2.0, "x=1,y=2,z=1"); // y=2 occurs three times with the same value for x, once with some other tag.
        H2SEvent.create(rbb.db(), 1.0, 2.0, "x=2,y=2"); // y=2 occurs with 2 different values for x
        H2SEvent.create(rbb.db(), 1.0, 2.0, "x=2,y=3");
        H2SEvent.create(rbb.db(), 1.0, 2.0, "y=4,z=2"); // y=4 occurs only with z, not with x
        H2SEvent.create(rbb.db(), 1.0, 2.0, "y=3,r=1"); // r never occurs with x
        H2STagset.toID(rbb.db(), "x=2,y=5"); // y=5 occurs in a tagset but not a tagset attached to an Event.

        ResultSet rs;
        HashSet<String> results;

        // all values of y that occur in all tagsets
        rs = H2STagset.findCombinations(rbb.db(), "y", null, null, null);
        results = new HashSet<String>();
        while(rs.next())
            results.add(rs.getString(1));
        assertTrue(results.contains("y=1"));
        assertTrue(results.contains("y=2"));
        assertTrue(results.contains("y=3"));
        assertTrue(results.contains("y=4"));
        assertTrue(results.contains("y=5"));
        assertEquals(5, results.size());

        // all values of y that occur in events
        rs = H2SEvent.findTagCombinations(rbb.db(), "y", null);
        results = new HashSet<String>();
        while(rs.next())
            results.add(rs.getString(1));
        assertTrue(results.contains("y=1"));
        assertTrue(results.contains("y=2"));
        assertTrue(results.contains("y=3"));
        assertTrue(results.contains("y=4"));
        assertEquals(4, results.size()); // no y=5 since it isn't an event tag.

        // try with more than one group tag (not just y but x,y combinations)
        rs = H2SEvent.findTagCombinations(rbb.db(), "x,y", null);
        results = new HashSet<String>();
        while(rs.next())
            results.add(rs.getString(1));
        assertTrue(results.contains("x=1,y=1"));
        assertTrue(results.contains("x=1,y=2"));
        assertTrue(results.contains("x=2,y=2"));
        assertTrue(results.contains("x=2,y=3"));
        assertEquals(4, results.size());

        // all values of y that occur with any value of x
        rs = H2SEvent.findTagCombinations(rbb.db(), "y", "x");
        results = new HashSet<String>();
        while(rs.next()) {
            results.add(rs.getString(1));
            System.err.println(rs.getString(1));
        }
        assertTrue(results.contains("x,y=1"));
        assertTrue(results.contains("x,y=2"));
        assertTrue(results.contains("x,y=3"));
        assertEquals(3, results.size());

        // all values of y that occur with x=1.
        // This also tests the order of the return values, since y=2 occurs twice and y=1 only once.
        rs = H2SEvent.findTagCombinations(rbb.db(), "y", "x=1");
        assertTrue(rs.next());
        assertEquals("x=1,y=2", rs.getString("TAGS"));
        assertEquals(3, rs.getInt("N")); // x=1,y=2 occurred three times.  (Once also with z but it is ignored)
        assertTrue(rs.next());
        assertEquals("x=1,y=1", rs.getString("TAGS"));
        assertEquals(1, rs.getInt("N"));
        assertFalse(rs.next());

        // repeat the previous test by calling as a SQL query, to test the SQL alias that should exist.
        rs = rbb.db().prepareStatement("call rbb_event_tag_combinations('y','x=1')").executeQuery();
        assertTrue(rs.next());
        assertEquals("x=1,y=2", rs.getString("TAGS"));
        assertEquals(3, rs.getInt("N")); // x=1,y=2 occurred three times.  (Once also with z but it is ignored)
        assertTrue(rs.next());
        assertEquals("x=1,y=1", rs.getString("TAGS"));
        assertEquals(1, rs.getInt("N"));
        assertFalse(rs.next());

        // test with a group tag (z) that is not always present with the others.
        rs = H2SEvent.findTagCombinations(rbb.db(), "y,z", "x");
        results = new HashSet<String>();
        assertTrue(rs.next());
        assertEquals("x,y=2,z=1", rs.getString("TAGS"));
        assertFalse(rs.next()); // all the tagsets with no value for z don't come into play

        // test with a group tag that exists but is never present with the filter tags
        rs = H2SEvent.findTagCombinations(rbb.db(), "r", "x");
        assertFalse(rs.next());

        // test with null group tags and a filter that matches some events
        System.err.println("boo");
        rs = H2SEvent.findTagCombinations(rbb.db(), null, "x=2");
        assertTrue(rs.next());
        assertEquals("x=2", rs.getString("TAGS"));
        assertEquals(2, rs.getInt("N"));
        assertFalse(rs.next()); // all the tagsets with no value for z don't come into play

        // test with null group tags and a filter that doesn't match any events
        rs = H2SEvent.findTagCombinations(rbb.db(), null, "wierdo");
        assertFalse(rs.next());

        // null group tags and null filter tags returns the empty tagset (which matches everything)
        rs = H2SEvent.findTagCombinations(rbb.db(), null, null);
        assertTrue(rs.next());
        assertEquals("", rs.getString("TAGS"));
        assertEquals(8, rs.getInt("N"));
        assertFalse(rs.next());

        // this counts all distinct tagsets whether or not they are attached to any event.
        // It is still 8 because one is not counted (it is a duplicate) but the Tagset not attached to an Event is counted.
        rs = H2STagset.findCombinations(rbb.db(), null, null, null, null);
        assertTrue(rs.next());
        assertEquals("", rs.getString("TAGS"));
        assertEquals(8, rs.getInt("N"));
        assertFalse(rs.next());

        // a tag can be in filterTags and group tags - here x is in both.
        rs = H2SEvent.findTagCombinations(rbb.db(), "x,y", "x=1");
        results = new HashSet<String>();
        while(rs.next())
            results.add(rs.getString(1));
        System.err.println(results);
        assertTrue(results.contains("x=1,y=1"));
        assertTrue(results.contains("x=1,y=2"));
        assertEquals(2, results.size());

        // use of a null-valued tag in filterTags doesn't change anything if it is already
        // in groupTags, but it is allowed.
        rs = H2SEvent.findTagCombinations(rbb.db(), "x,y", "x");
        results = new HashSet<String>();
        while(rs.next())
            results.add(rs.getString(1));
        System.err.println(results);
        assertTrue(results.contains("x=1,y=1"));
        assertTrue(results.contains("x=1,y=2"));
        assertTrue(results.contains("x=2,y=2"));
        assertTrue(results.contains("x=2,y=3"));
        assertEquals(4, results.size());

        // You can also just specify null-valued tags in filterTags instead
        // of specifying a list of tags to expand, if you want
        // ALL the null-valued tags in filterTags to be expanded.
        rs = H2SEvent.findTagCombinations(rbb.db(), "x,y");
        results = new HashSet<String>();
        while(rs.next())
            results.add(rs.getString(1));
        System.err.println(results);
        assertTrue(results.contains("x=1,y=1"));
        assertTrue(results.contains("x=1,y=2"));
        assertTrue(results.contains("x=2,y=2"));
        assertTrue(results.contains("x=2,y=3"));
        assertEquals(4, results.size());

        // now try a multi-valued tag name
        H2SEvent.create(rbb.db(), 1.0, 2.0, "x=1,x=2,x=3,y=1");
        H2SEvent.create(rbb.db(), 1.0, 2.0, "x=1,x=2,x=4,y=1");
        rs = H2SEvent.findTagCombinations(rbb.db(), "x,x", null);
        results = new HashSet<String>();
        while(rs.next())
            results.add(rs.getString(1)+":"+rs.getInt(2));
        assertTrue(results.contains("x=1,x=2:2"));
        assertTrue(results.contains("x=2,x=4:1"));
        assertTrue(results.contains("x=1,x=3:1"));
        assertTrue(results.contains("x=1,x=4:1"));
        assertTrue(results.contains("x=2,x=3:1"));

        rbb.disconnect();
    }


    @Test
    public void testFind()
        throws java.sql.SQLException
    {
        final String methodName =
            java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);
        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        final long id1 = H2STagset.toID(rbb.db(), "a=1,b=2");
        final long id2 = H2STagset.toID(rbb.db(), "a=1,c=3");
        final long id3 = H2STagset.toID(rbb.db(), "a=2,c=4");

        // base case of requring a single name/value pair
        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(rbb.db());
        H2STagset.hasTagsQuery(rbb.db(), "a=1", q);
        ResultSet rs = q.getPreparedStatement().executeQuery();
        Set<Long> results = new HashSet<Long>();
        while(rs.next())
            results.add(rs.getLong("TAGSET_ID"));
        assertEquals(2, results.size());
        assertTrue(results.contains(id1));
        assertTrue(results.contains(id2));

        // test with a tag name that is not in the database
        q = PreparedStatementCache.startQuery(rbb.db());
        H2STagset.hasTagsQuery(rbb.db(), "X=1", q);
        rs = q.getPreparedStatement().executeQuery();
        results = new HashSet<Long>();
        while(rs.next())
            results.add(rs.getLong("TAGSET_ID"));
        assertEquals(0, results.size());


        // test with a null tag value
        q = PreparedStatementCache.startQuery(rbb.db());
        H2STagset.hasTagsQuery(rbb.db(), "a=1,c", q);
        rs = q.getPreparedStatement().executeQuery();

        results = new HashSet<Long>();
        while(rs.next())
            results.add(rs.getLong("TAGSET_ID"));
        assertEquals(1, results.size());
        assertTrue(results.contains(id2));


        rbb.disconnect();

    }

    @Test
    public void IDTest()
        throws java.sql.SQLException
    {
        final String methodName =
            java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);
        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        final long id1 = H2STagset.toID(rbb.db(), "a=1,b=2");
        assertEquals(1, id1);

        // same tagset, but in differnt order.
        final long id2 = H2STagset.toID(rbb.db(), "b=2,a=1");
        assertEquals(1, id2);

        // creating a superset creates a different id
        final long id3 = H2STagset.toID(rbb.db(), "b=1,a=2,a=3");
        assertTrue(id1 != id3);

        // creating a subset creates a different id
        final long id4 = H2STagset.toID(rbb.db(), "b=1");
        assertTrue(id4 != id3 && id4 != id1);

        Tagset tags4 = new Tagset(H2STagset.fromID(rbb.db(), id4));
        assertEquals("1", tags4.getValue("b"));

        //// next line calls this through SQL -
        // Object[] tags34 = H2STagset.fromIDs(rbb.db(), new Object[]{id3, id4});
        //// instead, call through sql
        ResultSet rs = rbb.db().createStatement().executeQuery("call rbb_ids_to_tagsets(("+id3+","+id4+"));");
        assertTrue(rs.next());

        Object[] tags3and4 = (Object[]) rs.getArray(1).getArray();
        assertEquals(2, tags3and4.length);
        assertEquals("a=2,a=3,b=1", tags3and4[0]);
        assertEquals("b=1", tags3and4[1]);
        
        rbb.disconnect();
    }

    /**
     * This is testing the case of multi-valued tags, eg "x=1,x=2"
     * @throws java.sql.SQLException
     */
//    @Test
//    public void testFindCombinationsCheckPermutations()
//        throws java.sql.SQLException
//    {
//        final String methodName =
//            java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
//        System.err.println("Entering " + methodName);
//
//        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);
//
//        new Event(rbb.db(), 0, 1, TC("x=1,x=2,x=3,y=4"));
//
//        ResultSet rs = H2SEvent.findTagCombinations(rbb.db(), new Object[]{"x", null, "x", null, "y", null});
//
//        // order of return values is indeterminate so add to a set.
//        Set<String> results = new HashSet<String>();
//        while(rs.next())
//            results.add(rs.getString(1));
//
//        assertTrue(results.contains("x=1,x=2,y=4"));
//        assertTrue(results.contains("x=1,x=3,y=4"));
//        assertTrue(results.contains("x=2,x=3,y=4"));
//        assertEquals(3, results.size());
//
//        rbb.disconnect();
//
//    }

    @Test
    public void testSet()
        throws java.sql.SQLException
    {

        final String methodName =
            java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        // replace multiple existing values for 'b' with multiple new values,
        // but leeave 'a' alone, and add a new one, 'c'
        ResultSet rs =
            rbb.db().createStatement().executeQuery(
            "select rbb_set_tags('b=2,b=22,a=1', 'b=222,b=2222,c=3') as TAGS;");

        assert (rs.next());
        assertEquals("a=1,b=222,b=2222,c=3", rs.getString("TAGS"));

        rbb.disconnect();
    }

    @Test
    public void testTagNames()
        throws java.sql.SQLException
    {
         final String methodName =
            java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);
        new Event(rbb.db(), 0, 100, new Tagset("test1=1,test2=2,test3=3"));
        new Event(rbb.db(), 0, 100, new Tagset("test1=1,test2=2,test31=3"));


        String[] RBBResultNames = H2STagset.getAllTagNames(rbb.db());

        Set<String> resultNames = new HashSet<String>();
        resultNames.add("test1");
        resultNames.add("test2");
        resultNames.add("test3");
        resultNames.add("test31");


        for (int i=0;i<RBBResultNames.length;i++) {
            assertTrue(resultNames.contains(RBBResultNames[i]));
        }

        assertTrue(RBBResultNames.length == resultNames.size());

        rbb.disconnect();
    }


    @Test
    public void testEmptyTagset() throws SQLException
    {
       // Empty tagsets are a special case - see class documentation for H2STagset
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        assertEquals(0L, H2STagset.toID(rbb.db(), ""));
        assertEquals(0, H2STagset.fromID(rbb.db(), 0L).length());

        assertNull(H2STagset.find(rbb.db(), ""));

        rbb.disconnect();
    }

    @Test
    public void testNullValuedTag() throws SQLException
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        Long id = H2STagset.toID(rbb.db(), "name");

        assertEquals("name", H2STagset.fromID(rbb.db(), id));

        rbb.disconnect();

    }

}
