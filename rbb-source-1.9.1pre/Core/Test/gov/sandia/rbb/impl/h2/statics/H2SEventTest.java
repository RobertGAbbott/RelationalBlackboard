/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;

import java.util.Random;
import java.util.HashSet;
import java.sql.ResultSet;
import static gov.sandia.rbb.Tagset.TC;
import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;
import gov.sandia.rbb.*;
import static gov.sandia.rbb.RBBFilter.*;
import static gov.sandia.rbb.tools.RBBMain.RBBMain;
import static gov.sandia.rbb.impl.h2.statics.H2SRBBTest.*;

/**
 *
 * @author rgabbot
 */
public class H2SEventTest
{
    @Test
    public void testCreate()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, "testCreate");

        final long id1 = H2SEvent.create(rbb.db(), 1, 2, "name=hi,n=1");
        final long id2 = H2SEvent.create(rbb.db(), 100, 200, "name=hi,n=2");

        ResultSet rs;

        rs = rbb.db().createStatement().executeQuery(
            "select count(*) from RBB_EVENTS;");
        rs.next();
        assertEquals(2, rs.getInt(1));

        rs = rbb.db().createStatement().executeQuery(
            "select count(*) from RBB_EVENTS where START_TIME > 50;");
        rs.next();
        assertEquals(rs.getInt(1), 1);

        rbb.disconnect();
    }

    @Test
    public void testGetTags()  throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, "testGetTags");


        final long id1 = H2SEvent.create(rbb.db(), 1, 1, "z=Zack,z=Zodiak,a=1");
        assertEquals("a=1,z=Zack,z=Zodiak", H2SEvent.getTagsByID(rbb.db(), id1));

        H2SEvent.setTagsByID(rbb.db(), id1, "q=Quack,z=Zimbra");

        assertEquals("a=1,q=Quack,z=Zimbra", H2SEvent.getTagsByID(rbb.db(), id1));

        rbb.disconnect();
    }

    @Test
    public void testFindConcurrent()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, "testFindConcurrent");

        // 0  1  2  3  4  5  6  7  8  9
        // a  XXXXXXX  XXXX
        // b     XXXXXXX      XX
        // c  XXXXXXXXXXXXXXXXXXX

        H2SEvent.create(rbb.db(), 1, 3, "set=a");
        H2SEvent.create(rbb.db(), 4, 5.5, "set=a");

        H2SEvent.create(rbb.db(), 2, 4, "set=b");
        H2SEvent.create(rbb.db(), 6, 7, "set=b");

        H2SEvent.create(rbb.db(), 1, 7.5, "set=c");

        final double e = 1e-8; // epsilon

        // call through SQL to test SQL bindings in addition to the function itself
        String q = "call rbb_concurrent_events(('set=a','set=b', 'set=c'), null, 2.5, 100, null, null)";
//      H2SRBBTest.printQueryResults(rbb.db(), q);
//        ResultSet rs = rbb.db().createStatement().executeQuery(q);
        ResultSet rs = H2SEvent.findConcurrent(rbb.db(), Tagset.toTagsets("set=a;set=b;set=c"), null, 2.5, 100.0, null, null);

        rs.next();
        assertEquals(rs.getDouble(1), 2.5, e);
        assertEquals(rs.getDouble(2), 3.0, e);

        rs.next();
        assertEquals(rs.getDouble(1), 4.0, e);
        assertEquals(rs.getDouble(2), 4.0, e);

        assert (!rs.next());

        rbb.disconnect();
    }


    @Test
    public void testFindTagCombinations() throws Exception
    {
    
    }


    @Test
    public void testFindConcurrentWithTimeConversion()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        final double e = 1e-8; // epsilon

        Event evt = null;

        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=secondsSinceMidnight",
            1, 0); // in this example, the UTC is seconds since midnight.
        H2STime.defineCoordinate(rbb.db(),
            "timeCoordinate=minAfterHour,hour=1am", 1 / 60.0, -1 * 60);
        H2STime.defineCoordinate(rbb.db(),
            "timeCoordinate=minAfterHour,hour=2am", 1 / 60.0, -2 * 60);

        // create two events that overlap from 2:00 to 2:01 am

        evt = new Event(rbb.db(), 0, 61,
            new Tagset("test=x,name=1amTo2:01am,timeCoordinate=minAfterHour,hour=1am"));
//        System.err.println("created event " + evt.toString());

        evt = new Event(rbb.db(), 0, 61,
            new Tagset("test=x,name=2amTo3:01am,timeCoordinate=minAfterHour,hour=2am"));
//        System.err.println("created event " + evt.toString());

        String q;

        // calculate the period of overlap in terms of seconds since midnight (2:00 to 2:01)
//        q = "call rbb_concurrent_events((('a','1'),('b','2')), ('test=x', 'test=x'), null, null, 'timeCoordinate=secondsSinceMidnight')";
//        ResultSet rs0 = rbb.db().createStatement().executeQuery(q);
//        System.err.println("WHA?");

        ResultSet rs = H2SEvent.findConcurrent(rbb.db(), 
                new Object[]{"test=x", "test=x"}, null,
                null, null, "timeCoordinate=secondsSinceMidnight", null);

//        H2SRBBTest.printQueryResults(rbb.db(), q);
        assert (rs.next());
        assertEquals(rs.getDouble(1), 2 * 3600 + 60 * 0.0, e); // start time of overlap is 2:00
        assertEquals(rs.getDouble(2), 2 * 3600 + 60 * 1.0, e); // end   time of overlap is 2:01
        assert (!rs.next());

        // the same events overlap from 60 to 61 minutes past 1am
        q ="call rbb_concurrent_events(('test=x', 'test=x'), null, null, null, 'timeCoordinate=minAfterHour,hour=1am', null)";
//        H2SRBBTest.printQueryResults(rbb.db(), q);
        rs = rbb.db().createStatement().executeQuery(q);
        assert (rs.next());
        assertEquals(rs.getDouble(1), 60.0, e); // start time of overlap is 2:00
        assertEquals(rs.getDouble(2), 61.0, e); // end   time of overlap is 2:01
        assert (!rs.next());

        // the same events overlap from 0 to 1 minutes past 2am
        q = "call rbb_concurrent_events(('test=x', 'test=x'), null, null, null, 'timeCoordinate=minAfterHour,hour=2am', null)";
//        H2SRBBTest.printQueryResults(rbb.db(), q);
        rs = rbb.db().createStatement().executeQuery(q);
        assert (rs.next());
        assertEquals(rs.getDouble(1), 0.0, e); // start time of overlap is 2:00
        assertEquals(rs.getDouble(2), 1.0, e); // end   time of overlap is 2:01
        assert (!rs.next());

        rbb.disconnect();
    }

    @Test
    public void testAddTags()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);
        // RBB rbb = RBBFactory.getDefault().createRBB("jdbc:h2:file:///tmp/db/db");

        H2SEvent.create(rbb.db(), 1, 1, "id=000,type=even,n=1");
        H2SEvent.create(rbb.db(), 2, 2, "id=001,type=odd,n=2");
        H2SEvent.create(rbb.db(), 3, 3, "id=010,type=even,n=3");
        H2SEvent.create(rbb.db(), 4, 4, "id=011,type=odd,n=4");

        // call through SQL to test both the java implementation and the SQL binding.
        rbb.db().createStatement().executeQuery("call rbb_add_tags_to_events('type=even', 'newtag=wow,n=100');");
        
        String q = "call rbb_find_events(null, null, null, null);";

        ResultSet rs = rbb.db().createStatement().executeQuery(q);
        
        assertTrue(rs.next());
        assertEquals("id=000,n=1,n=100,newtag=wow,type=even", rs.getString("TAGS"));
        assertTrue(rs.next());
        assertEquals("id=001,n=2,type=odd", rs.getString("TAGS"));
        assertTrue(rs.next());
        assertEquals("id=010,n=100,n=3,newtag=wow,type=even", rs.getString("TAGS"));
        assertTrue(rs.next());
        assertEquals("id=011,n=4,type=odd", rs.getString("TAGS"));
        assertFalse(rs.next());

        rbb.disconnect();
    }

    @Test
    public void testFind()
        throws java.sql.SQLException
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);
        // RBB rbb = RBBFactory.getDefault().createRBB("jdbc:h2:file:///tmp/db/db");

        H2SEvent.create(rbb.db(), 110, 111, "subject=1,condition=1");
        H2SEvent.create(rbb.db(), 120, 121, "subject=1,condition=2");
        H2SEvent.create(rbb.db(), 130, 131, "subject=1,condition=3");

        H2SEvent.create(rbb.db(), 210, 211, "subject=2,condition=1");
        H2SEvent.create(rbb.db(), 220, 221, "subject=2,condition=2");

        // no filtering
        ResultSet rs = H2SEvent.find(rbb.db(), null, null, null, null);
        assertEquals("All rows should be returned", countRemainingRows(rs), 5);

        // query with empty tagset has same result as query with null tagset.
        rs = H2SEvent.find(rbb.db(), "", null, null, null);
        assertEquals("All rows should be returned", countRemainingRows(rs), 5);

        // filter on taglist only
        rs = H2SEvent.find(rbb.db(), "condition=2", null, null, null);
        assertTrue(
            "2 rows should be returned - if this is false, then there were 0",
            rs.next());
        Event ev = Event.getByID(rbb.db(), rs.getLong(1));
        assertEquals(120, ev.getStart(), 1e-8);
        assertTrue(
            "2 rows should be returned - if this is false, then there was only 1",
            rs.next());
        ev = Event.getByID(rbb.db(), rs.getLong(1));
        assertEquals(220, ev.getStart(), 1e-8);
        assertEquals(ev.getTagset().toString(), "condition=2,subject=2");
        assertEquals(
            "2 rows should have been returned, and we already extracted them", 0, countRemainingRows(
            rs));
        rs = H2SEvent.find(rbb.db(), "condition=2,subject=1", null, null, null);
        assertEquals("should return only 1 row", 1, countRemainingRows(
            rs));

        // filter out events ending before the start of the time of interest
        rs = H2SEvent.find(rbb.db(), null, 121.0, null, null);
        assertEquals(4, countRemainingRows(rs));
        rs = H2SEvent.find(rbb.db(), null, 121 + 1e-8, null, null);
        assertEquals(3, countRemainingRows(rs));

        // filter out events starting after the end of the time of interest.
        rs = H2SEvent.find(rbb.db(), null, null, 210.0, null);
        assertEquals(4, countRemainingRows(rs));
        rs = H2SEvent.find(rbb.db(), null, null, 210 - 1e-8, null);
        assertEquals(3, countRemainingRows(rs));

        // both filter tag and start time
        rs = H2SEvent.find(rbb.db(), "subject=1", null, 122.0, null);
        assertEquals(2, countRemainingRows(rs));

        // both filter tag and end time
        rs = H2SEvent.find(rbb.db(), "subject=1", null, 120.0, null);
        assertEquals(2, countRemainingRows(rs));

        // both start time and end time
        rs = H2SEvent.find(rbb.db(), null, 120.0, 120.0, null);
        assertEquals(1, countRemainingRows(rs));
        rs = H2SEvent.find(rbb.db(), null, 112.0, 113.0, null);
        assertEquals(0, countRemainingRows(rs));
        rs = H2SEvent.find(rbb.db(), null, 131.0, 210.0, null);
        assertEquals(2, countRemainingRows(rs));

        // all 3 - taglist, start time, and and time
        rs = H2SEvent.find(rbb.db(), "subject=1", 120.0, 300.0, null);
        assertEquals(2, countRemainingRows(rs));

        // H2SEvent.defineTimeCoordinatesForEventCombinations(rbb.db(), "sessionMilliseconds", null, "subject", 1000.0);

        rbb.disconnect();
    }

    @Test
    public void testFindWithTimeConversion()
        throws java.sql.SQLException
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        Event evt = null;

        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=secondsSinceMidnight",
            1, 0); // in this example, the UTC is seconds since midnight.
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=minAfterHour,hour=1am", 1 / 60.0, -1 * 60);

        evt = new Event(rbb.db(), 0, 61, new Tagset("test=x,name=1amTo201am,timeCoordinate=minAfterHour,hour=1am"));

        ResultSet rs = H2SEvent.find(rbb.db(), "test=x", 0.0, 60.0, "timeCoordinate=secondsSinceMidnight");
        assertEquals(0, countRemainingRows(rs)); // a result would be found if no time conversion were performed.

        rs = H2SEvent.find(rbb.db(), "test=x", 3610.0, 3611.0, "timeCoordinate=secondsSinceMidnight");
        // same query through SQL
        // H2SRBBTest.printQueryResults(rbb.db(), "call rbb_find_events(('test','x'), 3610, 3611, ('timeCoordinate','secondsSinceMidnight'));");
        // the result looks like:
        // ID        START_TIME        END_TIME        TAGS
        // 1         3600.0            7260.0          hour=1am,name=1amTo2:01am,test=x,timeCoordinate=secondsSinceMidnight
        assert (rs.next());
        assertEquals(evt.getID(), rs.getLong(1), 1e-8);
        assertEquals(3600.0, rs.getDouble(2), 1e-8);
        assertEquals(7260.0, rs.getDouble(3), 1e-8);
        assertEquals("hour=1am,name=1amTo201am,test=x,timeCoordinate=secondsSinceMidnight", rs.getString("TAGS"));

        // now limit by ID
        rs = H2SEvent.find(rbb.db(), "test=x", 3610.0, 3611.0, new Object[]{evt.getID()}, "timeCoordinate=secondsSinceMidnight", null);
        assertTrue(rs.next());
        assertFalse(rs.next());

        // now try with a false ID
        Long fakeID = evt.getID()+1;
        rs = H2SEvent.find(rbb.db(), "test=x", 3610.0, 3611.0, new Object[]{fakeID}, "timeCoordinate=secondsSinceMidnight", null);
        assertFalse(rs.next());

        rbb.disconnect();
    }

    @Test
    public void testDefineTimeCoordinatesForEventCombinations()
        throws java.sql.SQLException
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=SecondsUTC", 1.0, 0.0);

        // subject=1, condition=1
        H2SEvent.create(rbb.db(), 110, 118,
            "subject=1,condition=1,variable=x,timeCoordinate=SecondsUTC");
        H2SEvent.create(rbb.db(), 111, 119,
            "subject=1,condition=1,variable=y,timeCoordinate=SecondsUTC");
        H2SEvent.create(rbb.db(), 112, 118,
            "subject=1,condition=1,variable=z,timeCoordinate=SecondsUTC");

        // subject=1, condition=2
        H2SEvent.create(rbb.db(), 120, 128,
            "subject=1,condition=2,variable=x,timeCoordinate=SecondsUTC");
        H2SEvent.create(rbb.db(), 121, 129,
            "subject=1,condition=2,variable=y,timeCoordinate=SecondsUTC");
        H2SEvent.create(rbb.db(), 122, 128,
            "subject=1,condition=2,variable=z,timeCoordinate=SecondsUTC");

        // subject=2, condition=1
        H2SEvent.create(rbb.db(), 210, 218,
            "subject=2,condition=1,variable=z,timeCoordinate=SecondsUTC");
        H2SEvent.create(rbb.db(), 211, 219,
            "subject=2,condition=1,variable=y,timeCoordinate=SecondsUTC");
        H2SEvent.create(rbb.db(), 212, 216,
            "subject=2,condition=1,variable=x,timeCoordinate=SecondsUTC"); // assume x has a break for some reason
        H2SEvent.create(rbb.db(), 217, 218,
            "subject=2,condition=1,variable=x,timeCoordinate=SecondsUTC");

        // subject=2, condition=2
        H2SEvent.create(rbb.db(), 220, 228,
            "subject=2,condition=2,variable=x,timeCoordinate=SecondsUTC");
        H2SEvent.create(rbb.db(), 221, 229,
            "subject=2,condition=2,variable=y,timeCoordinate=SecondsUTC");
        H2SEvent.create(rbb.db(), 222, 228,
            "subject=2,condition=2,variable=z,timeCoordinate=SecondsUTC");

//        H2SRBBTest.printQueryResults(rbb.db(),
//            "select concat('subject=', rbb_id_to_string(subject_id)) as FILTER_TAGS from (select distinct (select VALUE_ID from rbb_tags_in_tagsets where TAGSET_ID = T.TAGSET_ID and NAME_ID = rbb_string_to_id('subject')) as subject_id from rbb_tags_in_tagsets T);");
//        H2SRBBTest.printQueryResults(rbb.db(),
//            "select concat('subject=', rbb_id_to_string(subject_id)) as FILTER_TAGS from (select distinct (select VALUE_ID from rbb_tags_in_tagsets where TAGSET_ID = T.TAGSET_ID and NAME_ID = rbb_string_to_id('subject')) as subject_id from rbb_tags_in_tagsets T);");

        //// start session at the start of the first variable for each subject, whichever variable that happens to be.
        // this code calls it directly but doesn't test the SQL binding, instead use the call uncommented below.
        //H2SEvent.defineTimeCoordinatesForEventCombinations(rbb.db(),
        //    "SessionMillis", new Object[]{"subject", null}, 1000.0);
        rbb.db().createStatement().executeQuery("call rbb_define_time_coordinates_for_event_combinations('SessionMillis', 'subject', null, 1000.0);");

        // retrieve the intercepts of the newly defined time coordinates.
        String q =
            "select INTERCEPT from RBB_TIME_COORDINATES where TIME_COORDINATE_STRING_ID = rbb_string_to_id('SessionMillis') order by INTERCEPT DESC;";
        // H2SRBBTest.printQueryResults(rbb.db(), q);
        ResultSet rs = rbb.db().createStatement().executeQuery(q);
        rs.next();
        assertEquals(rs.getDouble(1), -110000.0, 1e-8);
        rs.next();
        assertEquals(rs.getDouble(1), -210000.0, 1e-8);
        assertEquals("previously got the 2 results expected (one per subject)",
            0, countRemainingRows(rs));

        // now define a time coordinate for the start of each subject/condition pair, defined as starting with varible x.
        H2SEvent.defineTimeCoordinatesForEventCombinations(rbb.db(),
            "ConditionSeconds", "subject,condition", "variable=x", 1.0);
        q = "select INTERCEPT from RBB_TIME_COORDINATES where TIME_COORDINATE_STRING_ID = rbb_string_to_id('ConditionSeconds') order by INTERCEPT DESC;";
        rs = rbb.db().createStatement().executeQuery(q);
        rs.next();
        assertEquals(rs.getDouble(1), -110.0, 1e-8);
        rs.next();
        assertEquals(rs.getDouble(1), -120.0, 1e-8);
        rs.next();
        assertEquals(rs.getDouble(1), -212.0, 1e-8);
        rs.next();
        assertEquals(rs.getDouble(1), -220.0, 1e-8);
        assertEquals(
            "previously got the 4 results expected (one per subject/condition pair)",
            0, countRemainingRows(rs));

        //// view the final time coordinate table.
        // H2SRBBTest.printQueryResults(rbb.db(), "select *, rbb_id_to_string(TAGLIST_STRING_ID), rbb_id_to_string(TIME_COORDINATE_STRING_ID) from rbb_time_coordinates;");

        // test find that uses time conversions.
        // Maybe this should be in testFindEvents, but the bulk of the code to run this test is creating the sample data.
        // just get variable=x Events at least 5 seconds into each Condition
        rs = H2SEvent.find(rbb.db(), "variable=x", 5.0, 10.0,
            "timeCoordinate=ConditionSeconds");
        // There are 4 Condition/Subject pairs.
        // The start times are 0 (since we found variable=x, which defines the start of condition)
        //   except one variable contained a break, and the second event of variable=x started 5 seconds after the first.
        // Uncomment this to see the results:
        // System.err.println(H2SRBBTest.toString(rs));
        rs.next();
        assertEquals(rs.getDouble(2), 0.0, 1e-8);
        rs.next();
        assertEquals(rs.getDouble(2), 0.0, 1e-8);
        rs.next();
        assertEquals(rs.getDouble(2), 0.0, 1e-8);
        rs.next();
        assertEquals(rs.getDouble(2), 5.0, 1e-8);
        assertEquals(
            "previously got the 4 results expected (one per subject/condition pair)",
            0, countRemainingRows(rs));


        rbb.disconnect();
    }

    @Test
    public void testDelete() throws java.sql.SQLException
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        H2SEvent.create(rbb.db(), 0, 0, "n=1,c=2");
        H2SEvent.create(rbb.db(), 0, 0, "n=2,name=joe");
        H2SEvent.create(rbb.db(), 0, 0, "n=3,c=1");
        H2SEvent.create(rbb.db(), 0, 0, "n=4");

        ResultSet rs = rbb.db().createStatement().executeQuery("select count(*) from rbb_events");
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));

        // delete a single event
        H2SEvent.delete(rbb.db(), byTags("name=joe"));

        rs = rbb.db().createStatement().executeQuery("select count(*) from rbb_events");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));

        // delete events with a tag, regardless of value
        H2SEvent.delete(rbb.db(), byTags("c"));

        rs = rbb.db().createStatement().executeQuery("select count(*) from rbb_events");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

    }

    @Test
    public void testDeleteEventByID()
        throws java.sql.SQLException
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        final long e1 = H2SEvent.create(rbb.db(), 210, 218, "n=1");
        final long e2 = H2SEvent.create(rbb.db(), 210, 218, "n=2");
        final long e3 = H2SEvent.create(rbb.db(), 210, 218, "n=3");

        H2SEvent.deleteByID(rbb.db(), e2);

        ResultSet rs = rbb.db().createStatement().executeQuery(
            "select * from RBB_EVENTS order by ID");
        rs.next();
        assertEquals(e1, rs.getLong(1));
        rs.next();
        assertEquals(e3, rs.getLong(1));
        assertEquals("There should only be two rows", false, rs.next());

        rbb.disconnect();
    }

    @Test
    public void testSequence()
        throws java.sql.SQLException
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);
        Long id=-1L;

        id=H2SEvent.sequence(rbb.db(), 10.0, "name=Bob", "city=ABQ,weather=sunny");
        // this second event's tags are a subset of the first, so it should not create an additional event, just extend the first.
        id=H2SEvent.sequence(rbb.db(), 20.0, "name=Bob", "city=ABQ");
        id=H2SEvent.sequence(rbb.db(), 30.0, "name=Bob", "city=LA");
        id=H2SEvent.sequence(rbb.db(), 40.0, "name=Bob", "city=SLC");
        id=H2SEvent.sequence(rbb.db(), 50.0, "name=Bob", "city=ABQ");
        H2SEvent.setEndByID(rbb.db(), id, 100.0);

        ResultSet rs = H2SEvent.find(rbb.db(), "name=Bob", null, null, null);
        // System.err.println(H2SRBBTest.toString(rs));
        assertTrue(rs.next());
        assertEquals(10.0, rs.getDouble("START_TIME"), 1e-6);
        assertEquals(30.0, rs.getDouble("END_TIME"), 1e-6);
        assertEquals("city=ABQ,name=Bob,weather=sunny", rs.getString("TAGS"));

        assertTrue(rs.next());
        assertEquals(30.0, rs.getDouble("START_TIME"), 1e-6);
        assertEquals(40.0, rs.getDouble("END_TIME"), 1e-6);
        assertEquals("city=LA,name=Bob", rs.getString("TAGS"));

        assertTrue(rs.next());
        assertEquals(40.0, rs.getDouble("START_TIME"), 1e-6);
        assertEquals(50.0, rs.getDouble("END_TIME"), 1e-6);
        assertEquals("city=SLC,name=Bob", rs.getString("TAGS"));

        assertTrue(rs.next());
        assertEquals(50.0, rs.getDouble("START_TIME"), 1e-6);
        assertEquals(100.0, rs.getDouble("END_TIME"), 1e-6);
        assertEquals("city=ABQ,name=Bob", rs.getString("TAGS"));

        assertFalse(rs.next());

        // when was Bob in ABQ?
//        rs = H2SEvent.find(rbb.db(), "name=Bob,city=ABQ", null, null, null);
//        System.err.println(H2SRBBTest.toString(rs));
//
//        // where was Bob at t=33?
//        rs = H2SEvent.find(rbb.db(), "name=Bob", 33.0, 33.0, null);
//        System.err.println(H2SRBBTest.toString(rs));

        rbb.disconnect();
    }

    @Test
    public void testSetEnd()
        throws java.sql.SQLException
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        H2SEvent.create(rbb.db(), 1, 2, "n=1,evenodd=odd");
        H2SEvent.create(rbb.db(), 2, 3, "n=2,evenodd=even");
        H2SEvent.create(rbb.db(), 3, 4, "n=3,evenodd=odd");
        H2SEvent.create(rbb.db(), 4, 5, "n=4,evenodd=even");

        // int n = H2SEvent.setEnd(rbb.db(), "evenodd=even", 100);
        // assertEquals(2, n);

        ResultSet rs = rbb.db().createStatement().executeQuery(
            "call rbb_set_event_end('evenodd=even', 100.0);");

        rs = H2SEvent.find(rbb.db(), null, null, null, null);
        assertTrue(rs.next());
        assertEquals(2.0, rs.getDouble("END_TIME"), 1e-6);
        assertTrue(rs.next());
        assertEquals(100.0, rs.getDouble("END_TIME"), 1e-6);
        assertTrue(rs.next());
        assertEquals(4.0, rs.getDouble("END_TIME"), 1e-6);
        assertTrue(rs.next());
        assertEquals(100.0, rs.getDouble("END_TIME"), 1e-6);
        assertFalse(rs.next());

        rbb.disconnect();
    }

    @Test
    public void testSetEndByID()
        throws Throwable
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        H2SEvent.create(rbb.db(), 1, 2, "n=1,evenodd=odd");
        H2SEvent.create(rbb.db(), 2, 3, "n=2,evenodd=even");

        // int n = H2SEvent.setEnd(rbb.db(), "evenodd=even", 100);
        // assertEquals(2, n);

        ResultSet rs = rbb.db().createStatement().executeQuery(
            "call rbb_set_event_end_by_id(1, 100);");

        rs = H2SEvent.find(rbb.db(), null, null, null, null);
        assertTrue(rs.next());
        assertEquals(100.0, rs.getDouble("END_TIME"), 1e-6);
        assertTrue(rs.next());
        assertEquals(3.0, rs.getDouble("END_TIME"), 1e-6);
        assertFalse(rs.next());

        rbb.disconnect();
    }

    @Test
    public void testRemoveTags()
         throws Throwable
   {
        // invoke removeTags through sql.  This tests removeTags, removeTagsByID, and the SQL mapping.
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        final String jdbc = "jdbc:h2:mem:"+methodName;
        System.err.println("Entering "+methodName);
        RBB rbb = RBB.create(jdbc, null);

        H2SEvent.create(rbb.db(), 1, 2, "x=1,x=2,y=3,y=4,test=yes");
        H2SEvent.create(rbb.db(), 3, 4, "x=1,x=2,y=3,y=4,test=no");

        ResultSet rs = rbb.db().createStatement().executeQuery(
            "call rbb_remove_tags_from_events('test=yes', 'x=1,y,z=123');");

        rs = H2SEvent.find(rbb.db(), null, null, null, null);
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong("ID")); // H2SEvent returns events time-sorted order.
        assertEquals("test=yes,x=2", rs.getString("TAGS"));
        assertTrue(rs.next());
        assertEquals("test=no,x=1,x=2,y=3,y=4", rs.getString("TAGS")); // i.e., not changed.
        assertFalse(rs.next());

        // now test through RBB main
        RBBMain("removeTags", jdbc, "test=no", "y=3");
        rs = H2SEvent.find(rbb.db(), null, null, null, null);
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong("ID")); // H2SEvent returns events time-sorted order.
        assertEquals("test=yes,x=2", rs.getString("TAGS"));
        assertTrue(rs.next());
        assertEquals("test=no,x=1,x=2,y=4", rs.getString("TAGS")); // now y=3 is gone.
        assertFalse(rs.next());

        RBBMain("removeTags", jdbc, "", "x");
        rs = H2SEvent.find(rbb.db(), null, null, null, null);
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong("ID")); // H2SEvent returns events time-sorted order.
        assertEquals("test=yes", rs.getString("TAGS")); // all values of x now gone
        assertTrue(rs.next());
        assertEquals("test=no,y=4", rs.getString("TAGS")); // all values of x now gone.
        assertFalse(rs.next());

        rbb.disconnect();


    }


    @Test
    public void testFindNext()
         throws java.sql.SQLException
   {
        // invoke removeTags through sql.  This tests removeTags, removeTagsByID, and the SQL mapping.
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        H2SEvent.create(rbb.db(), 1, 10, "test=findNext");
        H2SEvent.create(rbb.db(), 2, 11, "test=findNext");
        H2SEvent.create(rbb.db(), 3, 12, "test=findNext");
        H2SEvent.create(rbb.db(), 3, 12, "test=findNext");
        H2SEvent.create(rbb.db(), 3, 12, "test=findNext");
        H2SEvent.create(rbb.db(), 4, 13, "test=findNext");

        ResultSet rs = rbb.db().createStatement().executeQuery("call rbb_find_next_event('test=findNext',3,null)");
        rs.next();
        assertEquals(4.0, rs.getDouble("START_TIME"), 1e-8);

        rs = rbb.db().createStatement().executeQuery("call rbb_find_prev_event('test=findNext',3,null)");
        rs.next();
        assertEquals(2.0, rs.getDouble("START_TIME"), 1e-8);


        rbb.disconnect();
    }

    @Test
    public void testAttachedDataTables() throws SQLException
    {
        // create both a timeseries and a bare event, and ensure only the timeseries
        // has an data table.

        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        final Long eventID = H2SEvent.create(rbb.db(), 1.0, 2.0, "type=event");

        String tsTags = "type=timeseries";
        final Long tsID = H2STimeseries.start(rbb.db(), 1, 3.0, tsTags);

        String[] eventTables = H2SEvent.attachedDataTables(rbb.db(), eventID, H2STimeseries.schemaName);

        String[] tsTables = H2SEvent.attachedDataTables(rbb.db(), tsID, H2STimeseries.schemaName);

        assertEquals(0, eventTables.length);

        assertEquals(1, tsTables.length);
        assertEquals("TF1", tsTables[0]);

        ResultSet rs = H2SEvent.find(rbb.db(), tsTags, null, null, null, null, H2STimeseries.schemaName);
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("DATA_COLS")); // 3 = time + 1d data + minustime
        assertFalse(rs.next());

        rbb.disconnect();
     
    }

    @Test
    public void testFindWithOr()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);
        final String dbURL = "jdbc:h2:mem:"+methodName;

        RBB rbb = RBB.create(dbURL, null);

        final Event e9 = new Event(rbb.db(), 9.0, 9.0, TC("x=d,y=4"));
        final Event e8 = new Event(rbb.db(), 8.0, 8.0, TC("x=c,y=3"));
        final Event e7 = new Event(rbb.db(), 7.0, 7.0, TC("x=b,y=2"));
        final Event e6 = new Event(rbb.db(), 6.0, 6.0, TC("x=a,y=1"));

        ResultSet rs = H2SEvent.find(rbb.db(), "x=d;y=2",
                null, null, null, null, null);

        assertTrue(rs.next());
        //System.err.println("tags returned = " + rs.getString("TAGS"));
        //System.err.println("Should be equal:\n\t"+e7.toString()+"\n\t"+Event.fromResultSet(rs).toString());
        assertTrue(e7.equals(new Event(rs)));

        assertTrue(rs.next());
        //System.err.println("Should be equal:\n\t"+e9.toString()+"\n\t"+Event.fromResultSet(rs).toString());
        assertTrue(e9.equals(new Event(rs)));

        assertFalse(rs.next());


        // a given event is not found multiple times even if it permutationIsSubset multiple "or' cases
        rs = H2SEvent.find(rbb.db(),"x=a,y=1;x=a",
                null, null, null, null, null);
        assertTrue(rs.next());
        assertTrue(e6.equals(new Event(rs)));
        assertFalse(rs.next());

        rbb.disconnect();
    }

    @Test
    public void testFindConcurrentWithOr()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);
        final String dbURL = "jdbc:h2:mem:"+methodName;

        RBB rbb = RBB.create(dbURL, null);

        final Event r1 = new Event(rbb.db(), 1.0, 2.0, TC("name=red1,n=1"));
        final Event r2 = new Event(rbb.db(), 3.0, 4.0, TC("name=red2,n=2"));
        final Event r3 = new Event(rbb.db(), 2.0, 7.0, TC("name=red3,n=3"));

        final Event b1 = new Event(rbb.db(), 1.0, 5.0, TC("name=blue1,n=1"));
        final Event b2 = new Event(rbb.db(), 1.1, 10.0, TC("name=blue2,n=2"));
        final Event b3 = new Event(rbb.db(), 11.0, 15.0, TC("name=blue3,n=3"));

        ResultSet rs = H2SEvent.findConcurrent(rbb.db(),
            new Object[]{ "name=red1;name=red2", "name=blue1;name=blue2;name=blue3"},
            null, null, null, null, null);

//        System.err.println(H2SRBBTest.toString(rs));
//1.0        2.0        [Ljava.lang.Long;@6db22920
//1.1        2.0        [Ljava.lang.Long;@4baa2c23
//3.0        4.0        [Ljava.lang.Long;@1137d4a4
//3.0        4.0        [Ljava.lang.Long;@686963d0

        assertTrue(rs.next());
        assertEquals(r1.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[0]);
        assertEquals(b1.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[1]);
        assertEquals(1.0, rs.getDouble("START_TIME"), 1e-8);
        assertEquals(2.0, rs.getDouble("END_TIME"), 1e-8);

        assertTrue(rs.next());
        assertEquals(r1.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[0]);
        assertEquals(b2.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[1]);
        assertEquals(1.1, rs.getDouble("START_TIME"), 1e-8);
        assertEquals(2.0, rs.getDouble("END_TIME"), 1e-8);

        assertTrue(rs.next());
        assertEquals(r2.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[0]);
        assertEquals(b1.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[1]);
        assertEquals(3.0, rs.getDouble("START_TIME"), 1e-8);
        assertEquals(4.0, rs.getDouble("END_TIME"), 1e-8);

        assertTrue(rs.next());
        assertEquals(r2.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[0]);
        assertEquals(b2.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[1]);
        assertEquals(3.0, rs.getDouble("START_TIME"), 1e-8);
        assertEquals(4.0, rs.getDouble("END_TIME"), 1e-8);

        assertFalse(rs.next());

        // now, findConcurrent with OR and "inherited" tag values (empty-valued tag)
        // The results are as previous, except r1 b2 does not match because blue2
        // refuses to match a red with a different value for n.
        rs = H2SEvent.findConcurrent(rbb.db(),
            new Object[]{ "name=red1;name=red2", "name=blue1;name=blue2,n=;name=blue3"},
            null, null, null, null, null);


        assertTrue(rs.next());
        assertEquals(r1.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[0]);
        assertEquals(b1.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[1]);
        assertEquals(1.0, rs.getDouble("START_TIME"), 1e-8);
        assertEquals(2.0, rs.getDouble("END_TIME"), 1e-8);

        // r1 / b2 will not match... b2 will only match after a red with the same n (2)
        
        assertTrue(rs.next());
        assertEquals(r2.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[0]);
        assertEquals(b1.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[1]);
        assertEquals(3.0, rs.getDouble("START_TIME"), 1e-8);
        assertEquals(4.0, rs.getDouble("END_TIME"), 1e-8);

        assertTrue(rs.next());
        assertEquals(r2.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[0]);
        assertEquals(b2.getID(), (Long)((Object[])rs.getArray("IDS").getArray())[1]);
        assertEquals(3.0, rs.getDouble("START_TIME"), 1e-8);
        assertEquals(4.0, rs.getDouble("END_TIME"), 1e-8);

        assertFalse(rs.next());


        rbb.disconnect();
    }

    @Test
    public void testGetCopiesByID()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);
        final String dbURL = "jdbc:h2:mem:"+methodName;

        RBB rbb = RBB.create(dbURL, null);

        final Event e9 = new Event(rbb.db(), 9.0, 9.0, TC("x=d,y=4"));
        final Event e8 = new Event(rbb.db(), 8.0, 8.0, TC("x=c,y=3"));

        // the main point here is ensuring the events are returned in the same
        // order their IDs are passed

        Event[] a1 = Event.getByIDs(rbb.db(), new Long[]{e8.getID(), e9.getID()});
        assertTrue(a1[0].equals(e8));
        assertTrue(a1[1].equals(e9));

        Event[] a2 = Event.getByIDs(rbb.db(), new Long[]{e9.getID(), e8.getID()});
        assertTrue(a2[0].equals(e9));
        assertTrue(a2[1].equals(e8));

        rbb.disconnect();
    }

    @Test
    public void testWeirdTagsets() throws SQLException {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        Random r = new Random();

        for(int iTagset = 0; iTagset < 1000; ++iTagset) {
            Tagset t = TagsetTest.makeRandomTagset(r);
            Event before = new Event(rbb.db(), iTagset, iTagset, t);

            Event after = Event.getByID(rbb.db(), before.getID());

            assertEquals(before, after);
            assertEquals(before.getTagset(), after.getTagset());
        } // end of creating tagsets

        rbb.disconnect();
    }

    @Test
    public void testClonePersistent() throws SQLException {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        long e1 = H2SEvent.create(rbb.db(), 1.0, 2.0, "n=1");
        long e2 = H2SEvent.create(rbb.db(), 3.0, 4.0, "n=2");
        long e3 = H2SEvent.clonePersistent(rbb.db(), e1, null, null, "N=3");

        Event[] a = Event.find(rbb.db()); // find all events
        assertEquals(3, a.length);

        assertTrue(e1 != e3);
        assertEquals(H2SEvent.getStartByID(rbb.db(), e1), H2SEvent.getStartByID(rbb.db(), e3), 1e-8);
        assertEquals(H2SEvent.getEndByID(rbb.db(), e1), H2SEvent.getEndByID(rbb.db(), e3), 1e-8);
        assertEquals("N=3,n=1", H2SEvent.getTagsByID(rbb.db(), e3));

        // make sure we know if we try to clone an invalid ID
        try {
            H2SEvent.clonePersistent(rbb.db(), 666L, null, null, "");
            assertTrue(false); // previous line should have thrown exception because 666L is not a valid Event ID
        }
        catch(IllegalArgumentException e) {
        }

        // now try with attached data, in this case a timeseries
        Timeseries ts1 = new Timeseries(rbb, 1, 4.0, new Tagset("n=3"));
        ts1.add(rbb, 4.0, 4.1f);
        ts1.add(rbb, 5.0, 5.1f);
        ts1.add(rbb, 6.0, 6.1f);
        ts1.setEnd(rbb.db(), 6.0);

        long ts2id = H2SEvent.clonePersistent(rbb.db(), ts1.getID(), null, null, "newTag=newValue");

        Timeseries ts2 = Timeseries.getByIDWithoutSamples(rbb.db(), ts2id);
        ts2.loadAllSamples(rbb.db());

        assertEquals(3, ts2.getNumSamples());

        for(int i = 0; i < ts1.getNumSamples(); ++i) {
            assertEquals(ts1.getSample(i).getTime(), ts2.getSample(i).getTime(), 1e-8);
            assertEquals(ts1.getSample(i).getValue()[0], ts2.getSample(i).getValue()[0], 1e-8f);
        }

        // now make a clone of a timeseries with a restricted start/end time

        long ts3id = H2SEvent.clonePersistent(rbb.db(), ts1.getID(), 4.5, 5.5, "newTag=newValue");

        Timeseries ts3 = Timeseries.getByIDWithoutSamples(rbb.db(), ts3id);
        ts3.loadAllSamples(rbb.db());

        assertEquals(4.5, ts3.getStart(), 1e-8); // start/end times were modified
        assertEquals(5.5, ts3.getEnd(), 1e-8);
        assertEquals(1, ts3.getNumSamples()); // only 1 sample survived.
        assertEquals(ts3.getSample(0).getTime(), 5.0, 1e-8);
        assertEquals(ts3.getSample(0).getValue()[0], 5.1f, 1e-8f);

        rbb.disconnect();
    }
}
