/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb;

import gov.sandia.rbb.TagsetComparator;
import gov.sandia.rbb.Tagset;
import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;
import static gov.sandia.rbb.Tagset.TC;
/**
 *
 * @author rgabbot
 */
public class TagsetComparatorTest {
    TagsetComparator c = new TagsetComparator();

    @Test
    public void testTagsetComparator() throws SQLException {

        assertEquals(0, c.compareStrings.compare(null, null));
        assert(c.compareStrings.compare(null, "") < 0);
        assert(c.compareStrings.compare("", null) > 0);
        assert(c.compareStrings.compare("", "x") < 0); // all non-null strings are handled by String.compareTo() which is reliable.

        assertBefore(TC(""), TC("a"));
        assertBefore(TC("a"), TC("a=1"));
        assertBefore(TC("a=1"), TC("a=2"));
        assertBefore(TC("a=10"), TC("a=2")); // comparison is lexicographical, not numerical
        assertBefore(TC("a=2"), TC("b=9,a=1")); // never gets to the point of comparing tag vaulues.
        assertBefore(TC("a=1,a=5"), TC("a=3"));
        assertBefore(TC("a=5,a=1"), TC("a=1,a=5,a=6"));

        c.compareAsNumbers("a");
        assertBefore(TC("a=2"), TC("a=10")); // comparison of a is now numerical.
        assertBefore(TC("a"), TC("a=-9999")); // null is allowable as a "number" and is before any actual number.

        try {
            assertBefore(TC("a=b"), TC("a=-9999")); // after we say to compare as a number, then we'll get an exception from parseDouble if any value is not a number (or null).
            assert(false); // shouldn't have got here.. should have raised an exception.
        } catch(NumberFormatException e) {
           //  System.err.println(e);
        } catch(Throwable t) {
            assert(false); // only expecting NumberFormatException.
        }

        c = new TagsetComparator();
        c.compareNumbersAsNumbers(TC("x"), TC("y"), TC("y=1,z=2"), TC("z=3,z=M,z=9"), TC("z=4"));
        assertEquals(1, c.compareAsNumbers.size());
        assertTrue(c.compareAsNumbers.contains("y"));

        // now try comparing only specified fields.
        c = new TagsetComparator();
        c.sortBy("x", "y");
        assertEquals(0, c.compare(TC("x=0,y=0,z=0"), TC("x=0,y=0,z=1")));
        assertBefore(TC("y=0"), TC("x=0,y=0"));
        assertBefore(TC("x=0,y=0"), TC("x=1,y=-1"));
        c.sortBy("y", "x");
         assertAfter(TC("x=0,y=0"), TC("x=1,y=-1"));

         // now try descending (reverse sort order)
        c = new TagsetComparator();
        c.sortDescending();
        assertBefore(TC("x=b"), TC("x=a"));
    }

    void assertBefore(Tagset a, Tagset b) {
        assert(c.compare(a, b) < 0);
        assert(c.compare(b, a) > 0);
    }

    void assertAfter(Tagset a, Tagset b) {
        assertBefore(b,a);
    }
}
