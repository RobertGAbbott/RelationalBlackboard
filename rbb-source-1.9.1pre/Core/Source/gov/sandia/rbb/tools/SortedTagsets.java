/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.tools;

import java.sql.ResultSet;
import gov.sandia.rbb.util.StringsWriter;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import java.util.Arrays;
import java.util.ArrayList;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.TagsetComparator;
import java.io.PrintStream;

/**
 *
 * Note: this class is not threadsafe- it should only be accessed
 * or manipulated from the Swing update thread.
 *
 * @author rgabbot
 */
public class SortedTagsets {

    private boolean printAsTable;
    private String insertCount;
    private boolean sortDescending;
    private Tagset[] tags;
    private String filterTags;
    private String[] tagOrdering;
    private RBB rbb;

    public SortedTagsets(RBB rbb) {
        setDefaults();
        this.rbb = rbb;
    }

    public String getTagName(int i) {
        return tagOrdering[i];
    }

    public int getNumTags() {
        return tagOrdering.length;
    }

    public int getNumTagsets() throws Exception {
        if(tags == null)
            update();
        return tags.length;
    }

    /*
     * Change the restriction on the set of Events to be displayed.
     * Does not change the columns displayed.
     * Only Events with BOTH these tags AND the tags corresponding to display columns will be displayed.
     */
    public void setFilterTags(String newFilterTags) {
        // System.err.println("SortedTagsets filterTags set to: " + newFilterTags);
        this.filterTags = newFilterTags;
        invalidate();
    }

    public String getFilterTags() {
        return filterTags;
    }

    /**
     *
     * @param tagset: indexes the sorted tagsets
     * @param tag: indexes the tag ordering
     * @return
     */
    public String getTagValue(int tagset, int tag) throws Exception {
        if(tags == null)
            update();
        return tags[tagset].getValue(tagOrdering[tag]);
    }

    /**
     * reset to defaults
     */
    public void setDefaults() {
        printAsTable = false;
        insertCount = null;
        sortDescending = false;
        tags = null;
        filterTags = null;
        tagOrdering = null;
    }

    /**
     * Trigger re-query before returning any subsequent get operation.
     */
    public void invalidate() {
        tags = null;
    }

    private void update() throws Exception {
        ResultSet rs = H2SEvent.findTagCombinations(rbb.db(), StringsWriter.join(",", tagOrdering), filterTags);
        ArrayList<Tagset> alist = new ArrayList<Tagset>();
        while(rs.next()) {
            Tagset t = new Tagset(rs.getString("TAGS"));
            if(insertCount != null)
                t.set(insertCount, Integer.toString(rs.getInt("N")));
            alist.add(t);
        }

        // sort
        TagsetComparator compare = new TagsetComparator();
        if(sortDescending)
            compare.sortDescending();
        compare.sortBy(tagOrdering);
        tags = alist.toArray(new Tagset[0]);
        compare.compareNumbersAsNumbers(tags);
        Arrays.sort(tags, compare);
    }

    public static final String recognizedArgs =
                "\t-count <name>: Insert a pseudo tag whose value is the number of events with this combination of tag values.  Will not be displayed unless included in tagName list.\n"+
                "\t-descending: Sort results in descending order, rather than default of ascending order\n"+
                "\t-filterTags <tagName=tagValue,...>: Report only tagsets for Events matching these tags\n"+
                "\t-table: Report each tag in a tab-delimited column\n"+
                "\ttagName1[,tagName2...]: List the columns (names of tags) that will be displayed\n";

    /*
     * Reset all settings to default and then take new settings from the args.
     * On error throws an exception with a succinct error message.
     * The caller may use the recognizedArgs string above to construct a full error message.
     */
    public void parseArgs(String... args) throws Exception {

        setDefaults();

        int iArg = 0;
        for (; iArg < args.length && args[iArg].startsWith("-"); ++iArg)
        {
            if(args[iArg].equalsIgnoreCase("-count")) {
                if(++iArg >= args.length-1)
                    throw new Exception("Error: -count requires a name to be used as a count.");
                insertCount = args[iArg];
            }
            else if(args[iArg].equalsIgnoreCase("-table")) {
                printAsTable = true;
            }
            else if(args[iArg].equalsIgnoreCase("-descending")) {
                sortDescending = true;
            }
            else if(args[iArg].equalsIgnoreCase("-filterTags")) {
                if(filterTags != null)
                    throw new Exception("Error: -filterTags can only be specified once.");
                if(++iArg >= args.length-1)
                    throw new Exception("Error: -filterTags requires a tagset as an arg.");
                filterTags = args[iArg];
            }
            else throw new Exception("Unrecognized arg \""+args[iArg]);
        }
        if(iArg == args.length)
            throw new Exception("Missing tag name list.");

        tagOrdering = args[iArg].split(",");
    }

    public void print(PrintStream ps) throws Exception {
        if(tags == null)
            update();

        Tagset tmp = new Tagset();

        if(printAsTable)
            ps.println(StringsWriter.join("\t", tagOrdering));

        for(Tagset t : tags) {
            if(printAsTable) {
                for(String tagName : tagOrdering) {
                    if(!tagName.equals(tagOrdering[0]))
                        ps.print("\t");
                    ps.print(t.getValue(tagName));
                }
                ps.println("");
            }
            else {
                // if -result is used, then t contains extra tags that should not be printed.
                for(String tagName : tagOrdering)
                    tmp.set(tagName, t.getValue(tagName));
                ps.println(tmp);
            }
        }
        
    }
}
