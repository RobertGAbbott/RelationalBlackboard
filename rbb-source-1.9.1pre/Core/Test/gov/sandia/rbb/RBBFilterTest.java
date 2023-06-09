/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb;

import java.util.ArrayList;
import java.util.Random;
import org.junit.Test;
import static org.junit.Assert.*;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */
public class RBBFilterTest {

    @Test
    public void testEquals() {
    }

    @Test
    public void testToStringFromString() {
        for(int i = 0; i < 10000; ++i) {
            final RBBFilter before = makeRandomRBBFilter();
            // System.err.println("-before--" + before + "--");
            final RBBFilter after = RBBFilter.fromString(before.toString());
            // System.err.println("+after+++" + after + "++");
            assertEquals(before, after);
        }

        // here is a corner case in which a tagset looks like a different RBBFilter field.
        RBBFilter f = new RBBFilter(byTags(byEnd(1.1).toString()));
        assertEquals(f, RBBFilter.fromString(f.toString()));


    }

    static Random rand = new Random();

    private RBBFilter makeRandomRBBFilter() {
        RBBFilter filter = new RBBFilter();

        if(rand.nextInt(4)==0) {
            final int numIDs = rand.nextInt(3);
            ArrayList<Long> ids = new ArrayList<Long>();
            for(int i = 0; i < numIDs; ++i)
                ids.add(rand.nextLong());
            filter.IDs = ids.toArray(new Long[0]);
        }

        if(rand.nextInt(4)==0)
            filter.attachmentInSchema = "Schema"+rand.nextInt(100);

        if(rand.nextInt(4)==0)
            filter.start = rand.nextDouble();

        if(rand.nextInt(4)==0)
            filter.end = rand.nextDouble();

        if(rand.nextInt(2)==0) {
            final int numTagsets = rand.nextInt(3);
            ArrayList<Tagset> tags = new ArrayList<Tagset>();
            for(int i = 0; i < numTagsets; ++i)
                tags.add(TagsetTest.makeRandomTagset(rand));
            filter.also(byTags(tags.toArray(new Tagset[0])));
        }

        if(rand.nextInt(4)==0)
            filter.timeCoordinate = new Tagset("timeCoordinate=years");

        return filter;
    }

}