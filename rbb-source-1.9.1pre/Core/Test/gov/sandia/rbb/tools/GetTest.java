
package gov.sandia.rbb.tools;

import java.io.*;
import org.junit.Test;
import static org.junit.Assert.*;
import gov.sandia.rbb.*;
import gov.sandia.rbb.impl.h2.statics.H2STime;

/**
 *
 * @author rgabbot
 */
public class GetTest {
    @Test
    public void testGet()
        throws Exception
    {
        System.err.println("Entering "+ java.lang.Thread.currentThread().getStackTrace()[1].getMethodName());

        final String dbURL = "jdbc:h2:mem:Test";
        RBB rbb = RBB.create(dbURL, null);
        Timeseries ts = null;

        // series c - x=t+3, y=t+3.1
        ts = new Timeseries(rbb, 2, 3.0, new Tagset("name=a,dim=2"));
        ts.add(rbb, 10.0, 13.0f, 13.1f);
        ts.add(rbb, 20.0, 23.0f, 23.1f);
        ts.add(rbb, 31.0, 34.0f, 34.1f);
        ts.setEnd(rbb.db(), 50.0);

        // series b - x=t+2, y=t+2.1
        ts = new Timeseries(rbb, 2, 2.0, new Tagset("name=b,dim=2"));
        ts.add(rbb, 10.0, 12.0f, 12.1f);
        ts.add(rbb, 20.0, 22.0f, 22.1f);
        ts.add(rbb, 30.0, 32.0f, 32.1f);
        ts.setEnd(rbb.db(), 51.0);

        // series a - x=t+1, y=t+1.1
        ts = new Timeseries(rbb, 2, 1.0, new Tagset("name=c,dim=2"));
        ts.add(rbb, 10.0, 11.0f, 11.1f);
        ts.add(rbb, 20.5, 21.0f, 21.1f);
        ts.add(rbb, 30.0, 31.0f, 31.1f);
        ts.setEnd(rbb.db(), 52.0);

       ByteArrayOutputStream output;

       //// test a 1-column get

       output = new ByteArrayOutputStream();
       Get.get(new PrintStream(output, true), "-tagsonly", dbURL, "dim=2" );

       // The output from rbb.tools.Get is deterministic because the tags and events are ordered.
       // the events are ordered by start time (which may be different than the time of the first observation!)
       // tags are ordered lexicographically, e.g. "dim=2,name=c" not "name=c,dim=2" because dim preceedes name in alphabetical order.

       // also note that extraneous output to System.out IS an error that needs to be fixed.
       // The command-line utilities output in a particular format that other programs count on.
       // Other messages (e.g. debugging or informational) need to go to System.err, or elsewhere.

       // System.err.println(output.toString());

       BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(output.toByteArray())));
       assertEquals("dim=2,end=52.0,name=c,start=1.0", reader.readLine());
       assertEquals("dim=2,end=51.0,name=b,start=2.0", reader.readLine());
       assertEquals("dim=2,end=50.0,name=a,start=3.0", reader.readLine());

       // get with an empty tagset
       output = new ByteArrayOutputStream();
       Get.get(new PrintStream(output, true), "-tagsonly", dbURL, "" );
       reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(output.toByteArray())));
       assertEquals("dim=2,end=52.0,name=c,start=1.0", reader.readLine());
       assertEquals("dim=2,end=51.0,name=b,start=2.0", reader.readLine());
       assertEquals("dim=2,end=50.0,name=a,start=3.0", reader.readLine());



       //// test a 2-column get

       output = new ByteArrayOutputStream();
       Get.get(new PrintStream(output, true), "-tagsonly", dbURL, "dim=2", "dim=2");
       // System.err.println(output.toString());
       reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(output.toByteArray())));
       assertEquals("dim=2,end=51.0,name=c,start=2.0 dim=2,end=51.0,name=b,start=2.0", reader.readLine());
       assertEquals("dim=2,end=50.0,name=c,start=3.0 dim=2,end=50.0,name=a,start=3.0", reader.readLine());
       assertEquals("dim=2,end=50.0,name=b,start=3.0 dim=2,end=50.0,name=a,start=3.0", reader.readLine());

       //// test a 2-column get with disjunctions
       output = new ByteArrayOutputStream();
       Get.get(new PrintStream(output, true), "-tagsonly", dbURL, "name=a", "name=b;name=c");
       //System.err.println("disjunctions:\n"+output.toString());
       reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(output.toByteArray())));
       assertEquals("dim=2,end=50.0,name=a,start=3.0 dim=2,end=50.0,name=b,start=3.0", reader.readLine());
       assertEquals("dim=2,end=50.0,name=a,start=3.0 dim=2,end=50.0,name=c,start=3.0", reader.readLine());

       //// test a 3-column get

       output = new ByteArrayOutputStream();
       Get.get(new PrintStream(output, true), "-tagsonly", dbURL, "dim=2", "dim=2", "dim=2");
       // System.err.println(output.toString());
       reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(output.toByteArray())));
       assertEquals("dim=2,end=50.0,name=c,start=3.0 dim=2,end=50.0,name=b,start=3.0 dim=2,end=50.0,name=a,start=3.0", reader.readLine());

       //// 2-column test with retrieval of data values (i.e., not using the -tagsonly this time)
       // this also tests interpolating values, since time times of observations for name=a are different than the others
       output = new ByteArrayOutputStream();
       Get.get(new PrintStream(output, true), "-notimes", dbURL, "dim=2", "dim=2");

       reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(output.toByteArray())));
       assertEquals("dim=2,name=c dim=2,name=b", reader.readLine());
       assertEquals("10.0,11.0,11.1 10.0,12.0,12.1", reader.readLine());
       assertEquals("20.5,21.0,21.1 20.5,22.5,22.6", reader.readLine());
       assertEquals("30.0,31.0,31.1 30.0,32.0,32.1", reader.readLine());
       assertEquals("", reader.readLine());
       assertEquals("dim=2,name=c dim=2,name=a", reader.readLine());
       assertEquals("10.0,11.0,11.1 10.0,13.0,13.1", reader.readLine());
       assertEquals("20.5,21.0,21.1 20.5,23.5,23.6", reader.readLine());
       assertEquals("30.0,31.0,31.1 30.0,33.0,33.1", reader.readLine());
       assertEquals("", reader.readLine());
       assertEquals("dim=2,name=b dim=2,name=a", reader.readLine());
       assertEquals("10.0,12.0,12.1 10.0,13.0,13.1", reader.readLine());
       assertEquals("20.0,22.0,22.1 20.0,23.0,23.1", reader.readLine());
       assertEquals("30.0,32.0,32.1 30.0,33.0,33.1", reader.readLine());
       assertEquals("", reader.readLine());
       assertEquals(false, reader.ready());
       
       rbb.disconnect();

    }

    @Test
    public void testGetWithTimeConversions()
        throws Exception
    {
        final String dbURL = "jdbc:h2:mem:Test2";
        RBB rbb = RBB.create(dbURL, null);

        Timeseries ts = null;
        ByteArrayOutputStream output = null;

        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=secondsSinceMidnight", 1, 0); // in this example, the reference time is seconds since midnight.
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=minutesSinceMidnight", 1/60.0, 0);
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=msAfterHour,hour=1am", 1000.0, -1*3600*1000.0);
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=msAfterHour,hour=2am", 1000.0, -2*3600*1000.0);
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=minBefore1am", 60.0, -3600.0);

        ts = new Timeseries(rbb, 1, 0, new Tagset("name=a,timeCoordinate=msAfterHour,hour=1am"));
        ts.add(rbb, 1 *  60 * 1000, 101.0f);   // 1:01 AM, 61 mins since midnight
        ts.add(rbb, 62 * 60 * 1000, 162.0f);  // 2:02 AM, 122 mins since midnight
        ts.setEnd(rbb.db(), 63 * 60 * 1000);  // 2:03 AM, 123 mins since midnight

        // note this timeseries starts at 30 seconds after 2am even though it's not observed until 2:01
        ts = new Timeseries(rbb, 1, 30*1000, new Tagset("name=b,timeCoordinate=msAfterHour,hour=2am"));
        ts.add(rbb, 1 *  60 * 1000, 1.0f);   // 2:01 AM, 121 mins since midnight
        ts.add(rbb, 50 * 60 * 1000, 50.0f);  // 2:50 AM, 170 mins since midnight
        ts.setEnd(rbb.db(), 50 * 60 * 1000);      // 2:50 AM, 170 mins since midnight

        // report concurrent events
        {
            output = new ByteArrayOutputStream();
            Get.get(new PrintStream(output, true), "-noTimes", "-timeCoordinate", "timeCoordinate=minutesSinceMidnight", dbURL, "name=a", "name=b");

            // since no timestep was specified, it will report at times
            // when the leftmost timeseries - name=a - was observed, that is, 2:02
            BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(output.toByteArray())));
            assertEquals("hour=1am,name=a,timeCoordinate=minutesSinceMidnight hour=2am,name=b,timeCoordinate=minutesSinceMidnight", reader.readLine());
            assertEquals("122.0,162.0 122.0,2.0", reader.readLine());
        }

        // report concurrent events
        {
            output = new ByteArrayOutputStream();
            Get.get(new PrintStream(output, true), "-noTimes", "-timestep", "1", "-timeCoordinate", "timeCoordinate=minutesSinceMidnight", dbURL, "name=a", "name=b");

            // since a timestep of 1 minute was specified, it will report at times
            // 2:00:30 (the start time of the second timeseries - 30 seconds before its first observation) and 2:02:30
            BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(output.toByteArray())));
            assertEquals("hour=1am,name=a,timeCoordinate=minutesSinceMidnight hour=2am,name=b,timeCoordinate=minutesSinceMidnight", reader.readLine());
            assertEquals("120.5,160.5 120.5,0.5", reader.readLine());
            assertEquals("121.5,161.5 121.5,1.5", reader.readLine());
            assertEquals("122.5,162.5 122.5,2.5", reader.readLine());
        }

        rbb.disconnect();

    }


    /*
     * A bug cropped up for a time conversion with a negative offset (converting to an earlier, rather than later time coordinate)
     */
    @Test
    public void testGetWithNegativeTimeConversions()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);
        final String dbURL = "jdbc:h2:mem:"+methodName;

        RBB rbb = RBB.create(dbURL, null);


        Timeseries ts = null;
        ByteArrayOutputStream output = null;

        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=seconds1AM", 1, 0); // in this example, the reference time is seconds since midnight.
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=secondsSinceMidnight", 1, 3600);

        ts = new Timeseries(rbb, 1, 0, new Tagset("name=a,timeCoordinate=secondsSinceMidnight"));
        ts.add(rbb, 1 *  60, 101.0f);   // 12:01 AM
        ts.add(rbb, 62 * 60, 162.0f);  // 1:02 AM
        ts.setEnd(rbb.db(), 63 * 60);  // 1:03 AM

        ts = new Timeseries(rbb, 1, 0, new Tagset("name=b,timeCoordinate=seconds1AM"));
        ts.add(rbb, 1 * 60, 101.0f);   // 1:01 AM
        ts.add(rbb, 2 * 60, 162.0f);  // 1:02 AM
        ts.setEnd(rbb.db(), 3 * 60);  // 1:03 AM

        // get the values
        {
            output = new ByteArrayOutputStream();
            Get.get(new PrintStream(output, true), "-noTimes", "-timeCoordinate", "timeCoordinate=seconds1AM", dbURL, "name=a", "name=b");


            // since no timestep was specified, it will report at times
            // when the leftmost timeseries - name=a - was observed, that is, 2:02
            BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(output.toByteArray())));
            assertEquals("name=a,timeCoordinate=seconds1AM name=b,timeCoordinate=seconds1AM", reader.readLine());
            assertEquals("120.0,162.0 120.0,162.0", reader.readLine());
        }

        rbb.disconnect();

    }

    /*
     * Get a mix of events and timeseries without specifying -tagsOnly
     */
    @Test
    public void testGetEvents()
        throws Exception
    {
        System.err.println("Entering "+ java.lang.Thread.currentThread().getStackTrace()[1].getMethodName());

        final String dbURL = "jdbc:h2:mem:Test";
        RBB rbb = RBB.create(dbURL, null);

        new Event(rbb.db(), 1, 3, new Tagset("name=1,description=event"));
        new Event(rbb.db(), 2, 4, new Tagset("name=2,description=event"));


        Timeseries ts = null;

        ts = new Timeseries(rbb, 1, 3.0, new Tagset("name=3,description=timeseries"));
        ts.add(rbb, 3.1, 3.11f);
        ts.add(rbb, 4.1, 4.11f);
        ts.setEnd(rbb.db(), 5.0);

       ByteArrayOutputStream output;

       //// test a 1-column get
       output = new ByteArrayOutputStream();
       Get.get(new PrintStream(output, true), dbURL, "" );

       // The output from rbb.tools.Get is deterministic because the tags and events are ordered.
       // the events are ordered by start time (which may be different than the time of the first observation!)
       // tags are ordered lexicographically, e.g. "dim=2,name=c" not "name=c,dim=2" because dim preceedes name in alphabetical order.

       // also note that extraneous output to System.out IS an error that needs to be fixed.
       // The command-line utilities output in a particular format that other programs count on.
       // Other messages (e.g. debugging or informational) need to go to System.err, or elsewhere.

       BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(output.toByteArray())));
       assertEquals("description=event,end=3.0,name=1,start=1.0", reader.readLine());
       assertEquals("description=event,end=4.0,name=2,start=2.0", reader.readLine());

       assertEquals("description=timeseries,end=5.0,name=3,start=3.0", reader.readLine());
       assertEquals("3.1,3.11", reader.readLine());
       assertEquals("4.1,4.11", reader.readLine());
       assertEquals("", reader.readLine());

       assertEquals(false, reader.ready());


       //// test a 2-column get - timeseries + event
       // only the observations falling within the concurrent time are retrieved.
       output = new ByteArrayOutputStream();
       Get.get(new PrintStream(output, true), dbURL, "name=3,description=timeseries", "name=2,description=event" );

       reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(output.toByteArray())));
       assertEquals("description=timeseries,end=4.0,name=3,start=3.0 description=event,end=4.0,name=2,start=3.0", reader.readLine()); // start/end times reflect period of overlap
       assertEquals("3.1,3.11 3.1", reader.readLine());
       assertEquals("", reader.readLine());

       assertEquals(false, reader.ready());


       // todo: ensure start/end time preserved when not same as sample times.

       rbb.disconnect();

    }


}
