/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import static gov.sandia.rbb.Tagset.TC;
import gov.sandia.rbb.Event;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class MLObservationTest {

    @Test
    public void testTagValueSubstitution() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);


        MLObservation.Metadata md = new MLObservation.Metadata();
        md.add("wingman", new Event(123L, 0.0, 100.0, TC("side=good,name=HanSolo,species=human")));

        assertEquals(TC("myside=good"), md.tagValueSubstitution(TC("myside=wingman.side")));
        assertEquals(TC("side=good"), md.tagValueSubstitution(TC("*=wingman.side")));
        assertEquals(TC("side=good,species=human,name=HanSolo"), md.tagValueSubstitution(TC("*=wingman.*")));
        assertEquals(TC("side=good,species=human,name=Luke"), md.tagValueSubstitution(TC("*=wingman.*,name=Luke")));
        assertEquals(TC("name=Luke,MyWingman.side=good,MyWingman.name=HanSolo,MyWingman.species=human"), md.tagValueSubstitution(TC("name=Luke,MyWingman.*=wingman.*")));
        assertEquals(TC("name=Luke,wingmanID=123"), md.tagValueSubstitution(TC("name=Luke,wingmanID=wingman.RBBID")));
        assertEquals(TC("name=Luke,name=HanSolo"), md.tagValueSubstitution(TC("name=Luke,name=wingman.name")));
    }

    @Test
    public void testCompareTo() throws Exception {
        final MLObservation a = new MLObservation(100.0, null);
        final MLObservation b = new MLObservation(110.0, null);
        assertEquals(-1, a.compareTo(b));
        assertEquals(1, b.compareTo(a));
        assertEquals(0, a.compareTo(a));
    }

}
