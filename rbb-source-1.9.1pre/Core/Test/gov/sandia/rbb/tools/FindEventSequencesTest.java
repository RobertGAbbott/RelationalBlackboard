
package gov.sandia.rbb.tools;

import java.util.Random;
import java.io.*;
import org.junit.Test;
import static org.junit.Assert.*;
import gov.sandia.rbb.*;
import static gov.sandia.rbb.Tagset.TC;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */
public class FindEventSequencesTest {

    @Test
    public void testFindEventSequences() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        long id1 = new Event(rbb.db(), 1.0, 2.0, TC("a=A,n=1")).getID();
        long id2 = new Event(rbb.db(), 2.0, 3.0, TC("a=B,n=1")).getID();
        long id3 = new Event(rbb.db(), 3.0, 4.0, TC("a=C,n=1")).getID();
        long id4 = new Event(rbb.db(), 4.0, 5.0, TC("a=A,n=2")).getID();
        long id5 = new Event(rbb.db(), 15.0, 16.0, TC("a=B,n=2")).getID();
        long id6 = new Event(rbb.db(), 16.0, 17.0, TC("a=C,n=2")).getID();
        long id7 = new Event(rbb.db(), 17.0, 18.0, TC("a=C,n=2")).getID();
        long id8 = new Event(rbb.db(), 18.0, 19.0, TC("foo=bar")).getID();

        FindEventSequences fes = new FindEventSequences();

        // by default, should simply find all events, in order.
        Event[][] groups = fes.getEventSequences(rbb);
        assertEquals(1, groups.length);
        assertEquals(8, groups[0].length);

        // use -filter but not -group
        fes = new FindEventSequences("-filter", "a");
        groups = fes.getEventSequences(rbb);
        assertEquals(1, groups.length);
        assertEquals(7, groups[0].length);

        fes = new FindEventSequences("-filter", "a=A");
        groups = fes.getEventSequences(rbb);
        assertEquals(1, groups.length);
        assertEquals(2, groups[0].length);

        // use -group but not -filter
        fes = new FindEventSequences("-group", "a");
        groups = fes.getEventSequences(rbb);
        assertEquals(3, groups.length);

        // try -timeout
        fes = new FindEventSequences("-timeout", "5");
        groups = fes.getEventSequences(rbb);
        assertEquals(2, groups.length);
        assertEquals(4, groups[0].length);
        assertEquals(4, groups[1].length);

        // try several args at once.
        fes = new FindEventSequences("-timeout", "5", "-filter", "a", "-group", "n");
        groups = fes.getEventSequences(rbb); // n=2 is found first because it occurs 4 times vs. only 3 for n=1.  But the n=2 group is split into 1 + 3 by the -timeout.
        assertEquals(3, groups.length);
        assertEquals(1, groups[0].length);
        assertEquals(3, groups[1].length);
        assertEquals(3, groups[2].length);

        // test -summarize
        fes = new FindEventSequences("-timeout", "5", "-filter", "a", "-group", "n", "-summarize", "n,hi=there,parentEvent");
        groups = fes.getEventSequences(rbb); // n=2 is found first because it occurs 4 times vs. only 3 for n=1.  But the n=2 group is split into 1 + 3 by the -timeout.
        assertEquals(3, groups.length);

        assertEquals(1, groups[0].length);
        assertEquals(1, groups[1].length);
        assertEquals(1, groups[2].length);

        assertEquals(4.0, groups[0][0].getStart(), 1e-6);
        assertEquals(5.0, groups[0][0].getEnd(), 1e-6);

        assertEquals(15.0, groups[1][0].getStart(), 1e-6);
        assertEquals(18.0, groups[1][0].getEnd(), 1e-6);

        assertEquals(1.0, groups[2][0].getStart(), 1e-6);
        assertEquals(4.0, groups[2][0].getEnd(), 1e-6);
        assertEquals("a=A,a=B,a=C,hi=there", groups[2][0].getTagset().toString());

        rbb.disconnect();
    }

    @Test
    public void testFindEventSequencesFilterAndGroup() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        final String dbURL = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(dbURL, null);

        // test when only some of all grouptags occur in the set specified by -filtertags.
        long id1 = new Event(rbb.db(), 1.0, 2.0, TC("a=1,b=1,b=2")).getID();
        long id2 = new Event(rbb.db(), 2.0, 3.0, TC("a=2,b=1,b=2")).getID();
        long id3 = new Event(rbb.db(), 3.0, 4.0, TC("a=2,b=2,b=3")).getID();

        FindEventSequences fes = new FindEventSequences();

        fes = new FindEventSequences("-filter", "a=1", "-group", "b,b");
        Event[][] events = fes.getEventSequences(rbb);
        assertEquals(1, events.length);
        assertEquals(1, events[0].length);
        assertEquals("a=1,b=1,b=2", events[0][0].getTagset().toString());

        rbb.disconnect();
    }

    //// this is actually a demo rather than a test, since the results aren't verified
    // @Test
    public void testGetEventSequencesRelational()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        final String dbURL = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(dbURL, null);

        // create a senso->commander->driver chain on fish1
        long id1 = new Event(rbb.db(), 1.0, 2.0, TC("type=radioCall,speakerRole=senso,platformType=submarine,platformCallsign=fish-1")).getID();
        long id2 = new Event(rbb.db(), 3.1, 4.0, TC("type=radioCall,speakerRole=commander,platformType=submarine,platformCallsign=fish-1")).getID();
        long id3 = new Event(rbb.db(), 7.0, 8.0, TC("type=radioCall,speakerRole=driver,platformType=submarine,platformCallsign=fish-1")).getID();

        // now a second senso->commander->driver chain on fish1, after a pause of 6 seconds from the first.
        long id4 = new Event(rbb.db(), 14.0, 15.0, TC("type=radioCall,speakerRole=senso,platformType=submarine,platformCallsign=fish-1")).getID();
        long id5 = new Event(rbb.db(), 16.0, 17.0, TC("type=radioCall,speakerRole=commander,platformType=submarine,platformCallsign=fish-1")).getID();
        long id6 = new Event(rbb.db(), 19.0, 20.0, TC("type=radioCall,speakerRole=driver,platformType=submarine,platformCallsign=fish-1")).getID();

        // now a senso->commander->driver chain on fish2, that overlaps in time with the ones on fish1
        long id7 = new Event(rbb.db(), 3.2, 5.0, TC("type=radioCall,speakerRole=senso,platformType=submarine,platformCallsign=fish-2")).getID();
        long id8 = new Event(rbb.db(), 9.0, 12.0, TC("type=radioCall,speakerRole=commander,platformType=submarine,platformCallsign=fish-2")).getID();
        long id9 = new Event(rbb.db(), 13.0, 15.0, TC("type=radioCall,speakerRole=driver,platformType=submarine,platformCallsign=fish-2")).getID();

        // now an ACO->CICO->RO chain on e2c-1
        long id10 = new Event(rbb.db(), 5.0, 7.0, TC("type=radioCall,speakerRole=ACO,platformType=e2c,platformCallsign=e2c-1")).getID();
        long id11 = new Event(rbb.db(), 8.0, 10.0, TC("type=radioCall,speakerRole=CICO,platformType=e2c,platformCallsign=e2c-1")).getID();
        long id12 = new Event(rbb.db(), 9.0, 11.0, TC("type=radioCall,speakerRole=RO,platformType=e2c,platformCallsign=e2c-1")).getID();

        // now some miscellaneous explosions.
        long id13 = new Event(rbb.db(), 1.1, 1.2, TC("type=explosion,entity=missile1")).getID();
        long id14 = new Event(rbb.db(), 3.3, 3.1, TC("type=explosion,entity=missile1")).getID();
        long id15 = new Event(rbb.db(), 5.1, 5.1, TC("type=explosion,entity=missile1")).getID();
        long id16 = new Event(rbb.db(), 12.0, 12.1, TC("type=explosion,entity=missile1")).getID();
        long id17 = new Event(rbb.db(), 14.0, 14.1, TC("type=explosion,entity=missile1")).getID();
        long id18 = new Event(rbb.db(), 15.0, 15.2, TC("type=explosion,entity=missile1")).getID();
        long id19 = new Event(rbb.db(), 18.0, 18.1, TC("type=explosion,entity=missile1")).getID();

       ByteArrayOutputStream output;

       output = new ByteArrayOutputStream();
//       System.setOut(new PrintStream(output, true));
//
//       // with just the URL, should just return all the events in time order.
//       gov.sandia.rbb.tools.FindEventSequences.main(
//               new String[] { dbURL }
//               );
//       assertTrue(output.toString().startsWith(""+
//        id1+ "\t1.0\t2.0\tplatformCallsign=fish-1,platformType=submarine,speakerRole=senso,type=radioCall\n"+
//        id13+"\t1.1\t1.2\tentity=missile1,type=explosion\n"+
//        id2+ "\t3.1\t4.0\tplatformCallsign=fish-1,platformType=submarine,speakerRole=commander,type=radioCall\n"+
//        id7+ "\t3.2\t5.0\tplatformCallsign=fish-2,platformType=submarine,speakerRole=senso,type=radioCall\n"+
//        id14+"\t3.3\t3.1\tentity=missile1,type=explosion\n"
//               ));

       // now, separate sequences for each submarine
        System.out.println("---");
        gov.sandia.rbb.tools.FindEventSequences.main(new String[] { dbURL } );
        System.out.println("---");
        gov.sandia.rbb.tools.FindEventSequences.main(new String[] {  "-filter", "platformType=submarine", "-timeout", "5", dbURL } );
        System.out.println("---");
        gov.sandia.rbb.tools.FindEventSequences.main(new String[] {  "-filter", "platformType=submarine", "-group", "platformCallsign", "-timeout", "5", dbURL } );
        System.out.println("---");

       rbb.disconnect();

    }

    //// this is actually a demo rather than a test, since the results aren't verified
    // @Test
    public void testGetEventSequences()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        final String dbURL = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(dbURL, null);

        String[] strings = { "happy", "birthday", "to", "you", null, "happy", "birthday", "to", "you", null, "happy", "birthday", "dear", "hailey", null, "happy", "birthday", "to", "you" };
        double t = 0.0;
        for(String s : strings)
            if(s == null)
                t += 6;
            else {
                new Event(rbb.db(), t, t+1, TC("word="+s));
                t += 2;
            }

        gov.sandia.rbb.tools.FindEventSequences.main(
             new String[] { dbURL }
             );

       System.out.println("---");
         gov.sandia.rbb.tools.FindEventSequences.main(
             new String[] { "-timeout", "5", dbURL }
             );

       System.out.println("---");
         gov.sandia.rbb.tools.FindEventSequences.main(
             new String[] { "-timeout", "5", "-summarize", "word,parentEvent,type=verseSummary", dbURL }
             );

       System.out.println("--- dear");
         gov.sandia.rbb.tools.FindEventSequences.main(
             new String[] { "-timeout", "5", "-regexTag", "word", "dear", dbURL }
             );

       System.out.println("--- dear");
         gov.sandia.rbb.tools.FindEventSequences.main(
             new String[] { "-timeout", "5", "-regexTag", "word", ".*dear.*", dbURL }
             );

      System.out.println("---");
         gov.sandia.rbb.tools.FindEventSequences.main(
             new String[] { "-timeout", "5", "-regexTag", "word", ".*dear.*", "-summarize", "", dbURL }
             );

      System.out.println("--- rh -- ");
         gov.sandia.rbb.tools.FindEventSequences.main(
             new String[] { "-timeout", "5", "-regexTag", "word", "rh", dbURL }
             );

         System.out.println("---");
         gov.sandia.rbb.tools.FindEventSequences.main(
             new String[] { "-timeout", "5", "-regexGroups", "word", "(birthday.*(you|hailey))", "1,2", dbURL }
             );

         System.out.println("-- birthday -");
         gov.sandia.rbb.tools.FindEventSequences.main(
             new String[] { "-timeout", "5", "-regexGroups", "word", "(birthday.*(you|hailey))", "2", dbURL }
             );

      System.out.println("---");


         rbb.disconnect();

    }


    //// this is actually a demo rather than a test, since the results aren't verified
    // @Test
    public void testFindEdits()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        final String dbURL = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(dbURL, null);

        String[] people = new String[] { "Joe", "Ted", "Bob", "Suzy" };
        String[] pages = new String[] { "Puppies.html", "IceCream.html", "GlobalJihad.html" };
        String[] editType = new String[] { "insert", "delete", "move" };
        String[] editSize = new String[] { "small", "medium", "large" };
        Random rand = new Random(2);
        for(int i = 0; i < 100; ++i) {
            double time = rand.nextInt(100);
            Tagset tags = new Tagset();
            tags.add("person", people[rand.nextInt(people.length)]);
            tags.add("page", pages[rand.nextInt(pages.length)]);
            tags.add("editType", editType[rand.nextInt(editType.length)]);
            tags.add("editSize", editSize[rand.nextInt(editSize.length)]);
            new Event(rbb.db(), time, time, tags);
        }

        System.out.println("----- find 'deletionists' whose edits (to any page) were all deletions at least 3 times successively");
        gov.sandia.rbb.tools.FindEventSequences.main(
             new String[] { "-group", "person", "-regexTag", "editType", "(delete){3,}", dbURL }
             );

        System.out.println("----- find minimal sequences in which people did small, then medium, then large edits to a given page.");
        gov.sandia.rbb.tools.FindEventSequences.main(
             new String[] { "-filter", "page=GlobalJihad.html", "-group", "person,page", "-regexTag", "editSize", "small.*?medium.*?large", dbURL }
             );

        System.out.println("-----summarize the small, medium, large edits from the previous query.");
        gov.sandia.rbb.tools.FindEventSequences.main(
             new String[] { "-filter", "page=GlobalJihad.html", "-group", "person,page", "-regexTag", "editSize", "small.*?medium.*?large", "-summarize", "", dbURL }
             );


        rbb.disconnect();

    }


    @Test
    public void testFindEventSequenceGaps() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        new Event(rbb.db(), 1.0, 3.0, TC("test=1,n=1,type=positive"));
        new Event(rbb.db(), 2.0, 4.0, TC("test=1,n=2,type=positive"));
        new Event(rbb.db(), 5.0, 6.0, TC("test=1,n=3,type=positive"));
        new Event(rbb.db(), 2.0, 3.0, TC("test=1,n=4,type=positive")); // falls entirely within other events so no effect

        FindEventSequences fes = new FindEventSequences();

        fes = new FindEventSequences(
            "-filter", "type=positive",
            "-gaps", "0", "10", "0.5", "type=negative",
            "-put"
            );
        
        Event[][] gaps = fes.getEventSequences(rbb);

        assertEquals(1, gaps.length);
        assertEquals(3, gaps[0].length);
        assertTrue(gaps[0][0].equals(new Event(0.0, 1.0, TC("n=1,test=1,type=negative"))));
        assertTrue(gaps[0][1].equals(new Event(4.0, 5.0, TC("n=2,test=1,type=negative"))));
        assertTrue(gaps[0][2].equals(new Event(6.0, 10.0, TC("n=3,test=1,type=negative"))));
        // now see if the -put option to the above actually stored the created events in the RBB.
        Event[] storedGaps = Event.find(rbb.db(), byTags("type=negative"));
        assertEquals(3, storedGaps.length);
        // copy the IDs from the stored events into the expected results.
        // This is not cheating since we know storing the events should create new IDs.
        assertTrue(storedGaps[0].equals(new Event(storedGaps[0].getID(), 0.0,1.0,TC("n=1,test=1,type=negative"))));
        assertTrue(storedGaps[1].equals(new Event(storedGaps[1].getID(), 4.0,5.0,TC("n=2,test=1,type=negative"))));
        assertTrue(storedGaps[2].equals(new Event(storedGaps[2].getID(), 6.0,10.0,TC("n=3,test=1,type=negative"))));

        // null start param - doesn't find a gap before first event.
        fes = new FindEventSequences(
            "-filter", "type=positive",
            "-gaps", "null", "10", "0.5", "type=negative"
            );
        gaps = fes.getEventSequences(rbb);
        assertEquals(1, gaps.length);
        assertEquals(2, gaps[0].length);
        assertTrue(gaps[0][0].equals(new Event(4.0, 5.0, TC("n=2,test=1,type=negative"))));
        assertTrue(gaps[0][1].equals(new Event(6.0, 10.0, TC("n=3,test=1,type=negative"))));

        // null end param - doesn't find a gap after last event.
        fes = new FindEventSequences(
            "-filter", "type=positive",
            "-gaps", "0", "null", "0.5", "type=negative"
            );
        gaps = fes.getEventSequences(rbb);
        assertEquals(1, gaps.length);
        assertEquals(2, gaps[0].length);
        assertTrue(gaps[0][0].equals(new Event(0.0, 1.0, TC("n=1,test=1,type=negative"))));
        assertTrue(gaps[0][1].equals(new Event(4.0, 5.0, TC("n=2,test=1,type=negative"))));

        // now try with a minGap parameter long enough to knock out all but one gap
        fes = new FindEventSequences(
            "-filter", "type=positive",
            "-gaps", "0", "10", "1.5", "type=negative"
            );

        gaps = fes.getEventSequences(rbb);
        assertEquals(1, gaps.length);
        assertEquals(1, gaps[0].length);
        assertTrue(gaps[0][0].equals(new Event(6.0, 10.0, TC("n=3,test=1,type=negative"))));

        // now try with a minGap parameter long enough to knock out all gaps
        fes = new FindEventSequences(
            "-filter", "type=positive",
            "-gaps", "0", "10", "5", "type=negative"
            );
        gaps = fes.getEventSequences(rbb);
        for(Event[] gapSequence : gaps) {
            for(Event gap : gapSequence) {
                System.err.println(gap.toString());
            }
            System.err.println("-");
        }
        assertEquals(1, gaps.length); // it still returns 1 result for the 1 group found
        assertEquals(0, gaps[0].length); // but that result has nothing in it.

        rbb.disconnect();
    }

}
