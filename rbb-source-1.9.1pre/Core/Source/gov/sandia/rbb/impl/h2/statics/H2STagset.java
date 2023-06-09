/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;

import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.PreparedStatementCache;
import gov.sandia.rbb.PreparedStatementCache.Delim;
import gov.sandia.rbb.util.StringsWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 *
 * The H2 implementation of RBB Tagsets.
 * The create_rbb.sql script creates aliases for these so they can be called through SQL.
 * The public functions here only accept / return datatypes for which H2 has a SQL mapping.
 *<p>
 * Special Case for Empty Tagset:
 * The SQL representation of H2STagset is the RBB_TAGSETS table, in which each
 * name/value pair is a row that contains the name, value, and tagset id.
 * This provides no way to store the empty tagset.
 * As a special case, the ID of the empty tagset is always 0.
 * fromID(0) will return the empty tagset even if it was never stored.
 * However, find() will never return 0, even if the empty tagset *has* been stored.
 * Similarly hasTagsQuery will never find the empty tagset, even if the query tagset is the empty tagset.
 *<p>
 * Implementation rationale: each tagset is stored in two tables: as a single string
 * in RBB_STRINGS, and in name/value components in RBB_TAGSETS.  (Then the names and values
 * are also stored in RBB_TAGSETS).  The single-string representation is for efficiently
 * checking if a given tagset already exists (when creating a new tagset) and for retrieving
 * a specified tagset by ID (such as when getting an RBB Event from the database).
 * The RBB_TAGSETS representation is for finding all tagsets that have a superset of specified names/values.
 *
 * @author rgabbot
 */
public class H2STagset
{
    /**
     * Get the ID for the tagset, creating a new ID if necessary.
     */
    public static long toID(Connection conn, String tagset_) throws java.sql.SQLException
    {
        if(tagset_.isEmpty())
            return 0L; // special case for empty tagset - see class documentation.

        Tagset tagset = new Tagset(tagset_);

        // the reason for converting tagset_ to a Tagset and back is to sort the
        // elements into the canonical order and escape special characters the same,
        // so equal tagsets will be identical.
        tagset_ = tagset.toString();

        Long id = H2SString.find(conn, tagset_);

        if(id != null)
            return id;

        id = H2SString.newID(conn, tagset_);

        PreparedStatementCache.Query ins = PreparedStatementCache.startQuery(conn);
        ins.addAlt("insert into RBB_TAGSETS values(RBB_STRING_TO_ID(",null,"), RBB_STRING_TO_ID(",null,"),",id,")");
        PreparedStatement ps = ins.getPreparedStatement();

        for(String name : tagset.getNames()) {
            for(String value : tagset.getValues(name)) {
                ps.setObject(1, name);
                ps.setObject(2, value);
                ps.addBatch();
            }
        }
        ps.executeBatch();

        return id;
    }

    /**
     * Adds a query that finds the IDs of all tagsets
     * with a superset of the specified tags.
     *<p>
     * Every tag must have a non-null name.
     * A null 'value' means no restriction on value.
     *
     * @param tagset
     * @return
     */
    public static void hasTagsQuery(Connection conn, String tagsets, PreparedStatementCache.Query q) throws SQLException {

        // if any of the Strings in the tagset is not in the RBB, then no tagset
        // will be found, so we just return this simple query that makes an empty table of tagset ids.
        final String emptyResult = "SELECT * FROM TABLE(TAGSET_ID BIGINT=())";

        Tagset[] tags = Tagset.toTagsets(tagsets);

        Object[] tagArray = new Object[tags.length];
        for(int i = 0; i < tags.length; ++i)
            tagArray[i] = tags[i].toArray();

        // each element of IDs is the String ID of the corresponding tag name/value in tags
        Object[] IDs = H2SString.findArray(conn, tagArray);

        UNION: for(int u = 0; u < tags.length; ++u) {
            if(u > 0)
                q.add(" union ");

            Object[] id = (Object[]) IDs[u];

            for(int i = 0; i < id.length; ++i) {
               if(id[i]==null) {
                   q.add(emptyResult);
                   continue UNION;
               }
            }

            q.add("select t0.tagset_id as TAGSET_ID from rbb_tagsets t0");
            for (int i = 2; i < id.length; i += 2)
                q.add(" join rbb_tagsets t"+i/2+" on t"+i/2+".tagset_id=t0.tagset_id");

            q.add(" where");
            for (int i = 0; i < id.length; i += 2) {
                if(i>0)
                   q.add(" and");
                q.add(" t"+i/2+".NAME_ID=");
                q.addParam(id[i]);
                if((Long) id[i+1] == 0L) // a string ID of 0 means the string was null, which means "don't care"
                    continue;
                q.add(" and t"+i/2+".VALUE_ID=");
                q.addParam(id[i+1]);
            }
        }
    }

    /**
     * Given a tagset, return the ID of the one tagset that *exactly* matches the
     * specified tagset (with no extra tags), or null if none.
     * @param conn
     * @param tagset
     * @return
     * @throws SQLException
     */
    public static Long find(Connection conn, String tagset) throws SQLException
    {
        if(tagset.length()==0)
            return null;
        return H2SString.find(conn, tagset);
    }

    /**
     * Convert a Tagset from the String representation "name1=value1,name2,name3="
     * to the Object[] representation { "name1","value1", "name2",null, "name3","" }
     * Tags are delimited by ','
     * If the tag contains one or more '=' characters, everything before the first '='
     * is the name, and everything after it is the value.
     * If the tag doesn't contain '=', the entire string is the name, and the value is null.
     *
     *
     * @param tagset
     * @return
     * @throws SQLException
     */
    public static Object[] stringToArray(String tagset) {
         return new Tagset(tagset).toArray();
    }


    /**
     * 
     * @param tags
     * @return
     */
    public static String arrayToString(Object[] tags) {
        return new Tagset(tags).toString();
    }


    public static String fromID(Connection conn, Long tagsetID) throws SQLException
    {
        if(tagsetID==0)
            return ""; // special case for empty tagset - see class documentation.

        return H2SString.fromID(conn, tagsetID);

    }

    public static Object[] fromIDs(Connection conn, Object[] tagsetIDs_) throws SQLException
    {
        Long[] tagsetIDs = H2SRBB.makeLongs(tagsetIDs_); // tagsetIDs may arrive as array of Int or array of Long
        String[] result = new String[tagsetIDs.length];
        for(int i = 0; i < tagsetIDs.length; ++i)
            result[i] = fromID(conn, tagsetIDs[i]);
        return result;
    }

    /**
     * Finds all combinations of tag values that co-occur in at least one tagset.
     *
     * returns two columns: TAGS and N (number of distinct tagsets that are a superset of this combination), ordered by descending N
     * TAGS           |  N
     * (subject, 30)  |	374
     *
     * @param tagNames is a comma-separated list of names of tags to be found co-occurring, such as "x" or "x,y"  or "x,y,y".
     *
     * @param filterTags is a tagset (name=value[,name2=value2...]).  All tagsets
     * not matching the filterTags will be ignored.  If it is null, no filtering is performed.
     *
     * @param inTable/inColumn restricts results to Tagsets whose ids are found in the specified table and column, e.g. "RBB_EVENTS.TAGSET_ID",
     *   and N is the number of times the combination appears in that column.  If null, uses a table of each distinct tagset ID that exists.
     *   This will even include (for example) tagsets that used to belong to events that have been deleted!
     * To find only tag combinations found in Events, use H2SEvent.findTagCombinations
     *
     * @throws java.sql.SQLException
     */
    public static ResultSet findCombinations(Connection conn, String tagNames_, String filterTags_, String inTable, String inColumn) throws SQLException {

        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);

        Tagset filterTags = new Tagset(filterTags_);
        String[] tagNames = tagNames_ == null ? new String[0] : tagNames_.split(",");

        // if a null-valued tag name is in filterTags and also in tagNames, it is redundant.
        for(String tagName : tagNames)
            filterTags.remove(tagName,null);

        Delim delim = Delim.CSV();
        q.add("select RBB_TAGSET_FROM_ARRAY((");
        for(int i=0; i < tagNames.length; ++i)
            q.add(delim.get(), "RBB_ID_TO_STRING(NAME"+i+"), RBB_ID_TO_STRING(VALUE"+i+")");
        for(String filterTagName : filterTags.getNames())
            for(String filterTagValue : filterTags.getValues(filterTagName))
                q.addAlt(delim.get(), filterTagName, ", ", filterTagValue);
        q.add(")) as TAGS, N from (");

        delim = Delim.CSV();
        q.add("select");
        for(int i = 0; i < tagNames.length; ++i)
            q.add(delim.get(), " N"+i+".NAME_ID as NAME"+i+", N"+i+".VALUE_ID as VALUE"+i);
        q.add(delim.get(), " count(*) as N from");
//        q.add(delim.get(), " count(*) as N from RBB_TAGSETS N0");

        // the base list of IDS to search
        q.add(" (");
        if(filterTags_ == null)
            q.add("select distinct TAGSET_ID from RBB_TAGSETS");
        else
            hasTagsQuery(conn, filterTags_, q);
        q.add(") IDS");


        if(inTable != null)
            q.add(" join ", inTable, " INTABLE on INTABLE.", inColumn,"=IDS.TAGSET_ID");

        for(int i = 0; i < tagNames.length; ++i)
            q.add(" join RBB_TAGSETS N"+i+" on N"+i+".TAGSET_ID=IDS.TAGSET_ID");

        Delim whereDelim = new Delim(" where", " and");
        for(int i = 0; i < tagNames.length; ++i) {
            q.addAlt(whereDelim.get()+" N"+i+".NAME_ID=", H2SString.find(conn, tagNames[i]));
            // prevent repeating permutations of the same tagset by checking for cases where
            // the same tagname occurs more than once, and in such cases returning
            // only in ascending order.
            for(int j = i-1; j >= 0; --j) {
                if(!tagNames[i].equals(tagNames[j]))
                    continue; // tag names are different, so no issue.
                q.add(" and N"+i+".VALUE_ID > N"+j+".VALUE_ID");
                break; // only needs to be > than nearest left predecessor with same tag name
            }
        }

        Delim groupDelim = new Delim(" group by", ",");
        for(int i = 0; i < tagNames.length; ++i)
            q.add(groupDelim.get(), " NAME"+i+", VALUE"+i);

        q.add(" order by N desc) where N > 0");

   //     System.err.println(q.toString());

        return q.getPreparedStatement().executeQuery();

    }


    /*
     * This is a simplified call to findCombinations in which tagNames and filterTags
     * are not specified separately; instead all null-valued tags in filterTags
     * are used as tagNames.  In other words, all null-valued tags are expanded.
     *<p>
     * This does sacrifice some expressive power, such as
     * findCombinations(db, "x", "y", ...) which means
     * "find all values for x that co-occur with ANY value of y" (but without expanding y).
     * It also precludes expanding multiple values for a tag that co-occur within a single
     * tagset, since a Tagset cannot have multiple null-valued tags of the same name.
     *<p>
     * This form is used by ui Draw and ui Timeline since otherwise the parameter lists
     * for them get awfully complicated.
     */
    public static ResultSet findCombinations(Connection conn, String filterTags_, String inTable, String inColumn) throws SQLException {

        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);

        Tagset filterTags = new Tagset(filterTags_);

        // if tagNames is not specified but filterTags is, then interpret null-valued tags
        // as tag names to expand.
        StringBuilder tagNames = new StringBuilder();
        Delim csv = Delim.CSV();
        if(filterTags_ != null)
            for(String name : filterTags.getNames())
                if(filterTags.contains(name,null))
                    tagNames.append(csv.get()+name);

        return findCombinations(conn, tagNames.toString(), filterTags.toString(), inTable, inColumn);
    }


    /**
     * returns 'oldTagsetString' after the removal any/all pairs with names in newTagsString, and the addition of all pairs in newTagsString
     * This doesn't change anything in the database - it just creates a new string from the two passed in.
     *
     * @param oldTagsetString
     * @param newTagsString
     * @return
     * @throws java.sql.SQLException
     */
    public static String set(String oldTags, String newTags) throws java.sql.SQLException
    {
        Tagset oldTagset = new Tagset(oldTags);
        Tagset newTagset = new Tagset(newTags);
        oldTagset.set(newTagset);
        return oldTagset.toString();
    }

    /**
     * throw a java.sql.SQLException with an explanitory note if
     * tags is not either null, or a valid tags array.
     */
    public static void checkNullOrTagArray(Object[] tags) throws java.sql.SQLException {
        if(tags != null)
            checkTagArray(tags);
    }

    /**
     * throw a java.sql.SQLException with an explanitory note if
     * tags is not either null, or a valid tags array, or an array of tag arrays.
     */
    public static void checkNullOrMultiTagArray(Object[] tags) throws java.sql.SQLException {
        if(tags == null)
            return;

        if(tags.length > 0 && tags[0] instanceof Object[]) {
            for(int i = 0; i < tags.length; ++i)
                checkTagArray((Object[])tags[i]);
            return;
        }

        checkTagArray(tags);
    }


    /**
     * throw a java.sql.SQLException with an explanitory note if 'tags'
     * is null or is not a valid tags array.
     */
    public static void checkTagArray(Object[] tags) throws java.sql.SQLException {
        try {
            if(tags == null) {
                throw new Exception("received a null tagset where not permitted");
            }
            if(tags.length % 2 != 0) {
                throw new Exception("received a tag array with "+tags.length+ " elements - must be an even number!");
            }
            for(int i = 0; i < tags.length; i+=2) {
                if(tags[i] == null) {
                    throw new Exception("tag name "+i+" is null!");
                    // tag values are allowed to be null in many contexts though.
                }
            }
        }
        catch(Exception e) {
            final String msg = e.getMessage();
            final String methodName = java.lang.Thread.currentThread().getStackTrace()[2].getClassName()+"."+java.lang.Thread.currentThread().getStackTrace()[2].getMethodName();
            final String example = "  Example: ('name1','val1', 'name2','val2')";
            final String stringTags = (tags != null && tags.length == 1 && tags[0].toString().indexOf('=') >= 0 ?
                "  It looks like you passed a string tagset (e.g. 'name1=value1,name2=value2') instead of a list alternating names and values?" : "");
            throw new java.sql.SQLException(methodName+": "+msg+stringTags+example);
        }
    }


    /**
     * Returns a list of all tagset names in RBB.
     * @return
     * @throws java.sql.SQLException
     */
    public static String[] getAllTagNames(Connection conn)
            throws java.sql.SQLException {
        String sqlStatement = "SELECT DISTINCT NAME_ID, STRING FROM RBB_TAGSETS, RBB_STRINGS WHERE NAME_ID = ID";
        java.sql.PreparedStatement prep = conn.prepareStatement(sqlStatement);
        ResultSet allNames = prep.executeQuery();
        
        List<String> tagNames = new ArrayList<String>();
        
        while (allNames.next()) {
            String tagname = allNames.getString(2);
            tagNames.add(tagname);
        }
        return(tagNames.toArray(new String[0]));
    }



}
