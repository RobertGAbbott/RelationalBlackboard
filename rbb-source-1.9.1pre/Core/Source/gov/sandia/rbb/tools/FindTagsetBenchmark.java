/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.tools;

import gov.sandia.rbb.RBB;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.impl.h2.statics.H2STagset;
import java.sql.SQLException;
import java.util.Date;
import java.util.Random;

/**
 * This class is being created as a benchmark for H2STagset.find which
 * has been determined by profiling to be unacceptably slow.
 *
 *
 * In the version checked in 2011 Fri Aug 19 1:25pm:
 *  Entering runTest
 *  FindTagsetBenchmark(100) took 11 milliseconds per tagset creation.
    Entering runTest
    FindTagsetBenchmark(200) took 9 milliseconds per tagset creation.
    Entering runTest
    FindTagsetBenchmark(300) took 13 milliseconds per tagset creation.
    Entering runTest
    FindTagsetBenchmark(400) took 15 milliseconds per tagset creation.
    Entering runTest
    FindTagsetBenchmark(500) took 18 milliseconds per tagset creation.
    Entering runTest
    FindTagsetBenchmark(1000) took 42 milliseconds per tagset creation.
 * 
 * Using the profiler takes 5m 46s to run:
 * 
 * FindTagsetBenchmark(200) took 1712 milliseconds per tagset creation.
 *
 * This benchmark calls H2STagset.toID.
 * Initial version spends 98.4% of its time in H2STagset.find
 * which is BAD since retrieving Events by tagset is so central to RBB.
 *
 * Is the query formed by H2STagset.hasTagsQuery even indexed?
 *
 * It appears that finding the tagset id by name and value is indexed:
 *
 * explain select TAGSET_ID from RBB_TAGSETS where NAME_ID=RBB_STRING_FIND_ID('name22') and VALUE_ID=RBB_STRING_FIND_ID('value64') intersect
 select TAGSET_ID from RBB_TAGSETS where NAME_ID=RBB_STRING_FIND_ID('name25') and VALUE_ID=RBB_STRING_FIND_ID('value32');
PLAN
(SELECT TAGSET_ID
FROM PUBLIC.RBB_TAGSETS /* PUBLIC.PRIMARY_KEY_4: NAME_ID = PUBLIC.RBB_STRING_FIND_ID('name22') AND VALUE_ID = PUBLIC.RBB_STRING_FIND_ID('value64')
WHERE (NAME_ID = PUBLIC.RBB_STRING_FIND_ID('name22')) AND (VALUE_ID = PUBLIC.RBB_STRING_FIND_ID('value64'))) INTERSECT (SELECT TAGSET_ID
FROM PUBLIC.RBB_TAGSETS /* PUBLIC.PRIMARY_KEY_4: NAME_ID = PUBLIC.RBB_STRING_FIND_ID('name25') AND VALUE_ID = PUBLIC.RBB_STRING_FIND_ID('value32')
WHERE (NAME_ID = PUBLIC.RBB_STRING_FIND_ID('name25')) AND (VALUE_ID = PUBLIC.RBB_STRING_FIND_ID('value32')))
(1 row, 1 ms)
 *
 * Is it the part that finds all tagsets with AT LEAST the specified tags (hasTagsQuery)
 * or the part that narrows that down to the one with EXACTLY the specified number of tags that's messed up?
 *
 * Do all the calls to string_find_id have a big effect?
 *
 * This query takes about 70 ms for 1 invocation!  468 on first invocation, 123 on second, then about 70.
 * call rbb_tagset_find_id(('name22', 'value91', 'name24', 'value97', 'name52', 'value52', 'name6', 'value44', 'name96', 'value27'));
 *
 * CREATE ALIAS has_tags_query FOR "gov.sandia.rbb.impl.h2.statics.H2STagset.hasTagsQuery";
 *
 *
  select TAGSET_ID from RBB_TAGSETS where NAME_ID=RBB_STRING_FIND_ID('name22')and VALUE_ID=RBB_STRING_FIND_ID('value91') intersect
  select TAGSET_ID from RBB_TAGSETS where NAME_ID=RBB_STRING_FIND_ID('name24')and VALUE_ID=RBB_STRING_FIND_ID('value97') intersect
  select TAGSET_ID from RBB_TAGSETS where NAME_ID=RBB_STRING_FIND_ID('name52')and VALUE_ID=RBB_STRING_FIND_ID('value52') intersect
  select TAGSET_ID from RBB_TAGSETS where NAME_ID=RBB_STRING_FIND_ID('name6')and VALUE_ID=RBB_STRING_FIND_ID('value44') intersect
  select TAGSET_ID from RBB_TAGSETS where NAME_ID=RBB_STRING_FIND_ID('name96')and VALUE_ID=RBB_STRING_FIND_ID('value27');
 *
 * whoah, the hasTagsQuery is taking 1 ms and the tagset_find_id is taking 58-100!!!  
 *
 * select rbb_id_to_tagset(tagset_id) from (select distinct tagset_id from rbb_tagsets);
 *
 * Now re-run the benchmark:
 *
 * Entering runTest
    FindTagsetBenchmark(100) took 4 milliseconds per tagset creation.
    Entering runTest
    FindTagsetBenchmark(200) took 1 milliseconds per tagset creation.
    Entering runTest
    FindTagsetBenchmark(300) took 0 milliseconds per tagset creation.
    Entering runTest
    FindTagsetBenchmark(400) took 0 milliseconds per tagset creation.
    Entering runTest
    FindTagsetBenchmark(500) took 0 milliseconds per tagset creation.
    Entering runTest
    FindTagsetBenchmark(1000) took 0 milliseconds per tagset creation.
 *
 * Yes!!!!!
 *
 */
public class FindTagsetBenchmark {

    private static void runTest(int numTagsets) throws SQLException {
        final int maxNumTags = 6; // max # tags in each tagset.

        Random rand = new Random(1974);

        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        final String URL = "jdbc:h2:tcp://localhost//tmp/mydb"+numTagsets;
        //final String URL = "jdbc:h2:mem:"+methodName+numTagsets;
        RBB rbb = RBB.create(URL, "test");

        for(int i = 0; i < numTagsets; ++i) {
            Tagset t = new Tagset();
            final int numTags = 1+rand.nextInt(maxNumTags);
            while(t.getNumTags() < numTags) { // may require > numTags tries since duplicates have no effect.
                t.add("name"+rand.nextInt(100), "value"+rand.nextInt(100));
            }
            H2STagset.toID(rbb.db(), t.toString());
        }

        rbb.disconnect();

    }

    public static void main(String[] args) throws Exception
    {
        //int[] n = new int[] { 100, 200, 300, 400, 500, 1000 };
        int[] n = new int[] { 200 };
        for(int n0 : n) {
            final long t0 = new Date().getTime();
            runTest(n0);
            final long elapsed = new Date().getTime()-t0;
            System.err.println("FindTagsetBenchmark("+n0+") took " + elapsed/n0 + " milliseconds per tagset creation.");
        }
    }

}
