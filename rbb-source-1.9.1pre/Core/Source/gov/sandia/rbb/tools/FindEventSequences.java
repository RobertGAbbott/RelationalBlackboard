
package gov.sandia.rbb.tools;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBFilter;
import static gov.sandia.rbb.RBBFilter.*;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.TagsetComparator;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.Timeseries.Sample;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Find sequences of events matching given patterns, ignoring intervening irrelevant events.
 * Run without args for a help string.
 * Also see gov.sandia.rbb.tools.FindEventSequencesTest.java.
 *
 * @author rgabbot
 */
public class FindEventSequences
{
    private static final String usage =
        "GetEventSequences [parameters...] JDBC_URL\n"
        + "   -filter <tagset>: ignore all RBB events that don't have (at least) these tags.\n"
        + "   -gaps <start> <end> <mingap> <setTags>: report gaps in each sequence rather than the sequence itself.\n"
        + "       The gaps are Events that inherit the tags of the previous Event in the sequence, with setTags then applied.\n"
        + "       mingap: ignore gaps not at least this long.\n"
        + "       start: if not null, a gap before the first event is reported starting at this time.  It inherits tags from the first in the sequence.\n"
        + "       end: if not null, a gap after the last event is reported ending at this time.\n"
        + "   -group <tagname[,tagName2...]>: each combination of values found for these tags will form a separate sequence of events.\n"
 //       + "   -addTag: <filterTagset> <addTagset>: add the specified tagset to any event matching the filterTagset\n"
 //       + "   -autoAddTags: tagname1[,tagname2...] keyName: for each combination of values for the named tags, generate a unique single-character value <v> and -addTags tagname1=value1.. keyName=<v>\n"
        + "   -phase <tagName,[tagName2...]>: Order Events in each sequence by the value of this tag, using the Event start time only to break ties among Events in the same phase (i.e. the same value for this tag).\n"
        + "   -put: store the output Events in the RBB (typically used with -summarize or -gaps because these synthesize new events.)\n"
        + "   -regexTag <tagname> <pattern>: for each sequence, concatenate values of the specified tag and output all sequences of events matching the regular expression.\n"
        + "   -regexGroups <tagname> <pattern> <1>[,2,...]: like -regexTag, but output the specified regex group(s) (this is for patterns with parens).\n"
        + "   -selected: ignore all RBB events that are not selected.\n"
        + "   -summarize <changeTagName=changeTagValue,...>: collapse each sequence into a single event.  The tags are the union of all events in the sequence, then with changeTags applied.\n"
        + "   -stack: remove gaps and overlaps by time-shifting each Event (starting with the second) to begin when the previous Event ends.\n"
        + "       If used with -phase, phases are stacked instead of individual events by starting each Event when the previous phase ends,\n"
        + "       with the duration of a phase being that of the longest Event in that phase, across all groups.\n"
        + "   -tagsOnly: Output timeseries as events (no Samples)\n"
        + "   -timeCoordinate <timeCoordinate=name,...>: use specified time coordinate for sorting and display.\n"
        + "   -timeout: start a new sequence of events whenever the difference between the end time of one event and the start time of the next exceeds this value.";

    String filterTags;

    String groupTags;

    boolean useSelected = false;

    boolean tagsOnly = false;

    /*
     * An ordered list of tag names.  The phase of each Event is defined by the combination of values it has for the phase tags.
     * This is stored as an array rather than a Tagset with null values because the order is significant;
     * phases are ordered by the value of the first tag, then the second, and so on.
     */
    String[] phaseTags;

    boolean stack = false;

    String timeCoordinate;

    ProblemEvaluator.PEChain peChain = new ProblemEvaluator.PEChain(null); // provide a do-nothing chain to build on.

    String jdbc;

    public FindEventSequences(String... args) throws Exception
    {
        int iArg = 0;

        ProblemEvaluator.PETimeout peTimeout = null;
        ProblemEvaluator.PERegex peRegex = null;
        ProblemEvaluator.PESummarize peSummarize = null;
        ProblemEvaluator.PEGaps peGaps = null;
        ProblemEvaluator.PEPut pePut = null;

        for (; iArg < args.length; ++iArg)
        {
            if (args[iArg].equalsIgnoreCase("-filter"))
            {
                ++iArg;
                if (iArg >= args.length)
                    throw new Exception("-filter requires a tagset");
                this.filterTags = args[iArg];
            }
            else if (args[iArg].equalsIgnoreCase("-group"))
            {
                ++iArg;
                if (iArg >= args.length)
                    throw new Exception("-group requires a comma-separated list of tag names, e.g. 'x' or 'x,y' or 'x,y,y'");

                this.groupTags = args[iArg];
            }
            else if (args[iArg].equalsIgnoreCase("-timeout"))
            {
                ++iArg;
                peTimeout = new ProblemEvaluator.PETimeout(Double.parseDouble(args[iArg]), null);
            }
            else if (args[iArg].equalsIgnoreCase("-regexTag")) {
                peRegex = new ProblemEvaluator.PERegex(args[iArg+1], args[iArg+2], new int[]{0}, null);
                iArg += 2;
            }
            else if (args[iArg].equalsIgnoreCase("-regexGroups")) {
                // convert groups indexes from strings to ints
                String[] groupStrings = args[iArg+3].split(",");
                int[] groups = new int[groupStrings.length];
                for(int i = 0; i < groups.length; ++i)
                    groups[i] = Integer.parseInt(groupStrings[i]);
                peRegex = new ProblemEvaluator.PERegex(args[iArg+1], args[iArg+2], groups, null);
                iArg += 3;
            }
            else if(args[iArg].equalsIgnoreCase("-phase")) {
                ++iArg;
                phaseTags = args[iArg].split(",");
            }
            else if(args[iArg].equalsIgnoreCase("-summarize")) {
                ++iArg;
                peSummarize = new ProblemEvaluator.PESummarize(new Tagset(args[iArg]), null);
            }
            else if(args[iArg].equalsIgnoreCase("-timeCoordinate")) {
                ++iArg;
                timeCoordinate = args[iArg];
            }
            else if(args[iArg].equalsIgnoreCase("-put")) {
                pePut = new ProblemEvaluator.PEPut(null);
            }
            else if(args[iArg].equalsIgnoreCase("-tagsOnly")) {
                tagsOnly = true;
            }
            else if(args[iArg].equalsIgnoreCase("-selected")) {
                useSelected = true;
            }
            else if(args[iArg].equalsIgnoreCase("-stack")) {
                stack = true;
            }
            else if(args[iArg].equalsIgnoreCase("-gaps")) {
                if (iArg+4 >= args.length)
                    throw new Exception("-gaps requires 4 args: <start> <end> <minGap> <changeTags>");

                peGaps = new ProblemEvaluator.PEGaps(null);

                ++iArg;
                if(!args[iArg].equalsIgnoreCase("null"))
                    peGaps.start = Double.parseDouble(args[iArg]);

                ++iArg;
                if(!args[iArg].equalsIgnoreCase("null"))
                    peGaps.end = Double.parseDouble(args[iArg]);

                ++iArg;
                if(!args[iArg].equalsIgnoreCase("null"))
                    peGaps.minGap = Double.parseDouble(args[iArg]);

                ++iArg;
                peGaps.changeTags = new Tagset(args[iArg]);

            }
//            else if (args[iArg].equalsIgnoreCase("-condense"))
//            {
//                ++iArg;
//                Tagset tags = new Tagset(args[iArg]);
//                ++iArg;
//                condense.put(tags, args[iArg]);
//                condenseKeys.add(tags);
//            }
//            else if (args[iArg].equalsIgnoreCase("-condenseAll"))
//            {
//                ++iArg;
//                condenseAll = args[iArg].split(",");
//            }
//            else if (args[iArg].equalsIgnoreCase("-oneLiners"))
//            {
//                oneLiners = true;
//            }
            else if (args[iArg].charAt(0) == '-')
            {
                throw new Exception("Unrecognized switch " + args[iArg]);
            }
            else
            {
                break;
            }
        }

        if (iArg < args.length)
        {
            jdbc = args[iArg];
            if (!jdbc.startsWith("jdbc:"))
            {
                System.err.println(jdbc
                    + " doesn't begin with 'jdbc:'; assuming jdbc:h2:file:" + jdbc);
                jdbc = "jdbc:h2:file:" + jdbc;
            }
        }

        // build the processing chain.
        if(peTimeout != null)
            this.peChain.addToChain(peTimeout);

        if(peRegex != null)
            this.peChain.addToChain(peRegex);

        if(peSummarize != null) {
            peSummarize.tagsOnly = tagsOnly;
            this.peChain.addToChain(peSummarize);
        }

        if(peGaps != null)
            this.peChain.addToChain(peGaps);

        if(pePut != null)
            this.peChain.addToChain(pePut);
    }

//    public void condenseAll() {
//        /// implement condenseAll
//        if (condenseAll != null)
//        {
//            Tagset condenseFilter = new Tagset();
//            for (String name : condenseAll)
//            {
//                condenseFilter.add(name, null);
//            }
//            if (filterTags != null)
//            {
//                condenseFilter.add(filterTags);
//            }
//            ResultSet rsTags = H2STagset.findCombinations(rbb.db(),
//                condenseFilter.toArray());
//            final int n = condense.size();
//            while (rsTags.next())
//            {
//                Tagset tags0 = new Tagset(
//                    (Object[]) rsTags.getArray(1).getArray());
//                if (condenseKeys.contains(tags0))
//                {
//                    continue; // don't auto-generate a code if the exact tagset already exists.
//                }
//                condenseKeys.add(tags0);
//                condense.put(tags0, condenseName(tags0.getValue(condenseAll[0]),
//                    condense.values()));
//            }
//            if (n == condense.size())
//            {
//                throw new Exception(
//                    "-condenseAll didn't find any values... do the -filter and -condenseAll tags exist in the data?");
//            }
//
//            // print the generated codes
//            for (Tagset t : condense.keySet())
//            {
//                System.err.println(condense.get(t) + "\t" + t.toString());
//            }
//        }
//    }

    public static void main(String[] args)
    {
        try
        {
            FindEventSequences fes = new FindEventSequences(args);
            if(fes.jdbc == null)
                throw new Exception("Error: the final arg must be a jdbc url: " + fes.usage);
            RBB rbb = RBB.connect(fes.jdbc);
            fes.peChain.addToChain(new ProblemEvaluator.PEPrinter(null)); // for this command-line invocation, the output is the console.
            fes.find(rbb.db());
            rbb.disconnect();
        }
        catch (Exception e)
        {
            System.err.println("Got Exception: " + e.getMessage());
            // e.printStackTrace();
        }
    }

    // this is the api for finding and retrieving event sequences in a 2d array.
    public Event[][] getEventSequences(RBB rbb) throws SQLException
    {
        ArrayList<Event[]> eventSequences = new ArrayList<Event[]>();
        this.peChain.addToChain(new ProblemEvaluator.PEAddToArray(eventSequences, null));
        this.find(rbb.db());
        return eventSequences.toArray(new Event[0][]);
    }

    /**
     * Find and process the sequences.
     *
     */
    void find(Connection db) throws SQLException
    {
        Phases phases = phaseTags == null ? null : new Phases();

        ResultSet rs = H2SEvent.findTagCombinations(db, groupTags, filterTags);
        while(rs.next()) {
            RBBFilter filter = byTags(rs.getString(1));
            if(useSelected)
                filter.also(byID(RBBSelection.oneShot(db, db).getSelectedEventIDs()));
            if(timeCoordinate != null)
                filter.also(withTimeCoordinate(timeCoordinate));
            Event[] events = Event.find(db, filter);

            if(phases == null)
                peChain.init(db, null, events);
            else
                phases.addGroup(events);
        }

        if(phases != null)
            for(Event[] group : phases.getGroups())
                peChain.init(db, null, group);
    }

    private static String condenseName(String seed,
        Collection<String> tabu)
        throws Exception
    {
        for (String c : seed.split(""))
        { // consider each character
            if (c.isEmpty())
            {
                continue;
            }
            if (!tabu.contains(c))
            {
                return c;
            }
            if (!tabu.contains(c.toUpperCase()))
            {
                return c.toUpperCase();
            }
            if (!tabu.contains(c.toLowerCase()))
            {
                return c.toLowerCase();
            }
        }
        for (char c = 'a'; c <= 'z'; ++c)
        {
            String s = new String(new char[]
                {
                    c
                });
            if (!tabu.contains(s))
            {
                return s;
            }
        }
        for (char c = 'A'; c <= 'Z'; ++c)
        {
            String s = new String(new char[]
                {
                    c
                });
            if (!tabu.contains(s))
            {
                return s;
            }
        }
        throw new Exception("condenseName ran out of single-character codes");
    }

/**
 *
 * An RBB "Problem" is a set of Events that when taken together match some defined criteria.
 *
 */
interface ProblemEvaluator {

    /***
     * Initialize a ProblemEvaluator with a specific set of events.
     */
    void init(Connection conn, Double time, Event[] events) throws java.sql.SQLException;

    /**
     * Each data[i][] is the most recently added data for the corresponding event (as passsed previously into init())
     *
     */
    void observe(Connection conn, Double time, Object[][] data) throws java.sql.SQLException;

    void done(Connection conn, Double time) throws java.sql.SQLException;

    /**
     * For ProblemListeners registered with RBB.addProblemListener(), clone() will
     * be invoked to make a new instance of this listener for every new problem
     * instance that occurs in the data.
     */
    ProblemEvaluator clone();


    /**
     * PEChain is a chain of problem evaluators.  It is the base class for specializations that do more significant processing.
     * It is used for batch processing, so init() does everything; observe() and done() do nothing.
     */
    class PEChain implements ProblemEvaluator {

        PEChain next;

        public PEChain(PEChain next) {
            this.next = next;
        }

        public void addToChain(PEChain last) {
            if(this.next == null)
                this.next = last;
            else
                this.next.addToChain(last);
        }

        @Override
        public void init(Connection conn,
            Double time,
            Event[] events)
            throws SQLException
        {
            if(this.next!=null)
                this.next.init(conn, time, events);
        }

        @Override
        public void observe(Connection conn,
            Double time,
            Object[][] data)
            throws SQLException
        {
            if(this.next != null)
                this.next.observe(conn, time, data);
        }

        @Override
        public void done(Connection conn,
            Double time)
            throws SQLException
        {
            if(this.next != null)
                this.next.done(conn, time);
        }

        @Override
        public ProblemEvaluator clone()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

    }

    /*
     * Print out the sequence of events.
     */
    class PEPrinter extends PEChain {
        PEPrinter(PEChain next) {
            super(next);
        }

        @Override
        public void init(Connection conn,
            Double time,
            Event[] events)
            throws SQLException
        {
            for(int i = 0; i < events.length; ++i) {
                final Event event = events[i];
                System.out.println("start="+event.getStart()+ ","+
                                   "end="+event.getEnd()+","+
                                   event.getTagset().toString());

                if(event instanceof Timeseries) {
                    for(Timeseries.Sample s : ((Timeseries)event).getSamples()) {
                        System.out.print(s.getTime());
                        for(Float x : s.getValue())
                            System.out.print(","+x);
                        System.out.println("");
                    }
                }
            }
            System.out.println(""); //empty line after each sequence.

            super.init(conn, time, events);
        }
    }

    /*
     * Store the events in the RBB, akin to RBB Put.
     */
    class PEPut extends PEChain {
        PEPut(PEChain next) {
            super(next);
        }

        @Override
        public void init(Connection conn,
            Double time,
            Event[] events)
            throws SQLException
        {
            for(Event event : events)
                H2SEvent.create(conn, event.getStart(), event.getEnd(), event.getTagset().toString());

            super.init(conn, time, events);
        }
    }

    /*
     * Make one event representing the whole sequence of events.
     * <p>
     * If any of the input Events are Timeseries, then the output Event is a Timeseries,
     * and has all samples from all input Timeseries.
     */
    class PESummarize extends PEChain {
        Tagset changeTags;
        boolean tagsOnly;
        PESummarize(Tagset changeTags, PEChain next) {
            super(next);
            this.changeTags = changeTags;
        }
        @Override
        public void init(Connection conn,
            Double time,
            Event[] events)
            throws SQLException
        {
            if(events.length == 0)
                throw new SQLException("PESummarize.init error: get empty array of Events");

            Event summary = new Event(events[0]); // inherit time conversion parameters from Event[0]
            summary.isPersistent = false;

            summary.setEnd(events[events.length-1].getEnd());

            summary.setTagset(Tagset.deriveTags(changeTags, Event.getTagsets(events)));

            // handle timeseries
            if(!tagsOnly) {
                events = Timeseries.promoteTimeseries(conn, events);
                ArrayList<Timeseries> ts = new ArrayList<Timeseries>();
                for(Event e : events)
                    if(e instanceof Timeseries)
                        ts.add((Timeseries)e);
                if(!ts.isEmpty()) {
                    final int dim = ts.get(0).getDim();
                    Timeseries summaryTS = new Timeseries(summary, dim);
                    for(Timeseries t : ts) {
                        if(t.getDim() != summaryTS.getDim())
                            throw new SQLException("PESummarize.init error: input timeseries have different dimension: "+summaryTS+" vs "+t);
                        t.loadAllSamples(conn);
                        for(Sample s : t.getSamples())
                            summaryTS.add(s);
                    }
                    summary = summaryTS;
                }
            }

            super.init(conn, time, new Event[]{ summary });
        }


    }



    class PEGaps extends PEChain {
        Double start;
        Double end;
        Double minGap;
        Tagset changeTags;

        PEGaps(PEChain next) {
            super(next);
        }

        @Override
        public void init(Connection conn,
            Double time,
            Event[] events)
            throws SQLException
        {
            // todo: set tags.

            // a gap is a period of time in which NONE of the Events is occurring.
            // Keep in mind, although Events are given in order of start time, an event
            // may end before, during, or after subsequent Events.
            // In contrast, the Gaps identified cannot overlap in time.

            // So the approach taken is to start with all of time as a single Gap,
            // then subdivide and delete Gaps in response to each Event.

            // Special case if we're asked to find the gaps in an empty set of events.
            // This situation is pretty nonsensical, but if start and end are specified, we'll
            // create a gap, and its only tags will be the changeTags.
            // If start or end is null, no gap is created.
            if(events.length == 0) {
                if(start != null && end != null)
                    super.init(conn, time, new Event[]{new Event(null, start, end, changeTags)});
                else
                    super.init(conn, time, new Event[0]);
                return;
            }


            ArrayList<Event> gaps = new ArrayList<Event>();
            // since the first gap has no previous Event, it gets its tagset from the earliest Event as a special case.
            gaps.add(new Event(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, gapTagset(events[0].getTagset())));

            for(Event event : events) {
                // find the last gap to start before this event starts.
                int iGap = 0;
                while(iGap < gaps.size()-1 && gaps.get(iGap+1).getStart() < event.getStart())
                    ++iGap;
                int iGapBefore = iGap;
                // find the first gap that ends after this event ends.
                while(iGap < gaps.size()-1 && gaps.get(iGap+1).getEnd() > event.getEnd())
                    ++iGap;
                int iGapAfter = iGap;

                if(iGapBefore==iGapAfter) { // the single gap is subdivided by the Event
                    ++iGapAfter;
                    gaps.add(iGapAfter, new Event(gaps.get(iGapBefore)));

                }

                // shrink the time bounds of the gaps before and after if necessary.

                if(gaps.get(iGapAfter).getStart() < event.getEnd()) {
                    gaps.get(iGapAfter).setStart(event.getEnd());
                    // gaps get their tagset from the event immediately previous.
                    gaps.get(iGapAfter).setTagset(gapTagset(event.getTagset()));
                }

                if(gaps.get(iGapBefore).getEnd() > event.getStart()) {
                    gaps.get(iGapBefore).setEnd(event.getStart());
                }

                // any gaps covered completely by the event, no longer exist.
                for(int iRemove = iGapAfter-1; iRemove > iGapBefore; --iRemove)
                    gaps.remove(iRemove);

            }

            // remove the initial gap, which starts at minus infinity.
            // If start==null, there is no gap before the first Event.
            if(start == null)
                gaps.remove(0);
            else
            {
                while(gaps.get(0).getEnd() < start)
                    gaps.remove(0);
                gaps.get(0).setStart(start);
            }

            // remove the final gap, which ends at infinity
            // if end==null, there is no gap after the last Event.
            if(end == null)
                gaps.remove(gaps.size()-1);
            else
            {
                while(gaps.get(gaps.size()-1).getStart() > end)
                    gaps.remove(gaps.size()-1);
                gaps.get(gaps.size()-1).setEnd(end);
            }

            // now apply minGap
            if(minGap != null) {
                for(int i = gaps.size()-1; i >= 0; --i)
                    if(gaps.get(i).getEnd()-gaps.get(i).getStart() < minGap)
                        gaps.remove(i);
            }

            super.init(conn, time, gaps.toArray(new Event[0]));
        }

        private Tagset gapTagset(Tagset eventTags) {
            Tagset t = new Tagset(eventTags);
            t.set(changeTags);
            return t;
        }

    }


    /**
     * Split the sequence of events if the time between them is >= timeout.
     */
    class PETimeout extends PEChain {
        Double timeout;
        PETimeout(Double timeout, PEChain next) {
            super(next);
            this.timeout = timeout;
        }
        @Override
        public void init(Connection conn,
            Double time,
            Event[] events)
            throws SQLException
        {
            int start = 0;
            for(int i = 0; i < events.length; ++i) {
                if(i > 0 && events[i].getStart()-events[i-1].getEnd() >= this.timeout) {
                    super.init(conn, events[start].getStart(), Arrays.copyOfRange(events, start, i));
                    start = i;
                }
            }
            super.init(conn, events[start].getStart(), Arrays.copyOfRange(events, start, events.length));
        }
    }

    /*
     * Replace the incoming sequence with a sequence for each regex match.
     *
     * if whichGroups is null, the entire match and all groups are printed.
     *
     */
    class PERegex extends PEChain {
        String regexTag;
        Pattern regex;
        int[] whichGroups;
        PERegex(String regexTag, String regex, int[] whichGroups, PEChain next) {
            super(next);
            this.regexTag = regexTag;
            this.regex = Pattern.compile(regex);
            this.whichGroups = whichGroups;
        }
        @Override
        public void init(Connection conn,
            Double time,
            Event[] events)
            throws SQLException
        {
           ByteArrayOutputStream bs = new ByteArrayOutputStream();
           PrintStream ps = new PrintStream(bs, true);
           int[] length = new int[events.length];

            for(int i = 0; i < events.length; ++i) {
                ps.print(events[i].getTagset().getValue(this.regexTag));
                length[i] = bs.size();
        //        System.err.println("value of " + this.regexTag + " in " + seq[i].getTagset().toString() + " is " + seq[i].getTagset().getValue(this.regexTag) + " now length is " + length[i]);
            }

            for(Matcher m = this.regex.matcher(bs.toString()); m.find(); ) {
    //            int group = 0;
    //            if(m.groupCount()>0)
    //                group = 1;
    //            for(; group <= m.groupCount(); ++group) {
                for(int group : this.whichGroups) {
                int eventA = Arrays.binarySearch(length, m.start(group));
    //            int eventA = Arrays.binarySearch(length, m.start());
                if(eventA >= 0) // starting character of match is the first character of the subsequent tag value
                    ++eventA;
                else
                    eventA = -eventA-1;

                int eventB = Arrays.binarySearch(length, m.end(group)-1); // end-1 because end indexes first char *after* end of match.
    //            int eventB = Arrays.binarySearch(length, m.end()-1); // end-1 because end indexes first char *after* end of match.
                if(eventB >= 0) // starting character of match is the first character of the subsequent tag value
                    ++eventB;
                else
                    eventB = -eventB-1;

    //            System.out.format("I found the text \"%s\" starting at " +
    //                   "index %d in event %d and ending at index %d in event %d.%n",
    //                    m.group(), m.start(), eventA, m.end(), eventB);

             //   System.err.println("GroupCount=" + m.groupCount());

                super.init(conn, events[eventA].getStart(), Arrays.copyOfRange(events, eventA, eventB+1));
                //printSequence(seq, eventA, eventB+1, false);
                }
            }


            // System.err.println("OK: " + bs.toString());

            // super.init(conn, time, events);
        }
    }

    /**
     * Add all the event sequences to an array.
     */
    class PEAddToArray extends PEChain {
        ArrayList<Event[]> eventSequences;

        PEAddToArray(ArrayList<Event[]> eventSequences, PEChain next) {
            super(next);
            this.eventSequences = eventSequences;
        }

        @Override
        public void init(Connection conn,
            Double time,
            Event[] events)
            throws SQLException
        {
            this.eventSequences.add(events);
            super.init(conn, time, events);
        }
    }
    }

    class Group extends ArrayList<Event> { };

    class Phase {
        double duration = 0.0;
        ArrayList<Group> groups = new ArrayList<Group>();
        void addEvent(int group, Event e) {
            while(groups.size() <= group)
                groups.add(new Group());
            groups.get(group).add(e);
            duration = Math.max(duration, e.getEnd()-e.getStart());
        }
    }

    class Phases extends HashMap<Tagset, Phase> {
        Tagset phaseKeyTemplate;
        int numGroups;
        Phases() {
            phaseKeyTemplate = new Tagset();
            for(String tagName : phaseTags)
                phaseKeyTemplate.add(tagName, null);
        }
        void addGroup(Event... events) {
            for(Event e : events) {
                Tagset phaseKey = Tagset.template(phaseKeyTemplate, e.getTagset());
                Phase p = get(phaseKey);
                if(p==null)
                    put(phaseKey, p=new Phase());
                p.addEvent(numGroups, e);
            }
            ++numGroups;
        }

        /*
         * This must be called only after you're done calling addGroup
         * result[i] is the i'th group
         */
        Event[][] getGroups() {
            Event[][] result = new Event[numGroups][];

            // now that we have seen all the phases determine their order.
            Tagset[] orderedPhaseKeys = keySet().toArray(new Tagset[0]);
            TagsetComparator phaseKeyComparator = new TagsetComparator();
            phaseKeyComparator.sortBy(phaseTags);
            phaseKeyComparator.compareNumbersAsNumbers(orderedPhaseKeys);
            Arrays.sort(orderedPhaseKeys, phaseKeyComparator);

            for(int iGroup=0; iGroup < numGroups; ++iGroup) {
                ArrayList<Event> eventsInGroup = new ArrayList<Event>(); // events in this group, in order
                double endOfPrevPhase = 0.0;
                for(Tagset phaseKey : orderedPhaseKeys) {
                    Phase phase = get(phaseKey);
                    if(iGroup < phase.groups.size() && phase.groups.get(iGroup) != null) {
                        Group phaseOfGroup = phase.groups.get(iGroup);
                        if(stack) {
                            for(Event e : phaseOfGroup) {
                                e.isPersistent = false;
                                e.timeShift(endOfPrevPhase - e.getStart());
                            }
                        }
                        Collections.sort(phaseOfGroup); // sort Events within the Phase by their default ordering, which is by start time.
                        eventsInGroup.addAll(phaseOfGroup);
                    }
                    endOfPrevPhase += phase.duration;
                }
                result[iGroup] = eventsInGroup.toArray(new Event[0]);
            }
            return result;
        }
    }

}
// java -cp $H2:$RBB RBB.util.Get jdbc:h2:tcp://localhost//tmp/db/db

