/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.tools;

import gov.sandia.rbb.Event;
import java.io.PrintStream;
import java.io.ByteArrayOutputStream;
import gov.sandia.rbb.RBB;
import static gov.sandia.rbb.Tagset.TC;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class SortedTagsetsTest {
    @Test
    public void testGetTagsets() throws Throwable {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        final String url = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(url, null);

        new Event(rbb.db(), 0.0, 1.0, TC("x=0,y=1,z=0"));
        new Event(rbb.db(), 0.0, 1.0, TC("x=1,y=0,z=1"));

        test(rbb, "x=0\nx=1\n", "x");
        test(rbb, "y=0\ny=1\n", "y");
        test(rbb, "x=1\nx=0\n", "-descending", "x");
        test(rbb, "x=0,y=1\nx=1,y=0\n", "-descending", "y,x"); // order in which tagnames were specified is not preserved when printing as tagsets
        test(rbb, "x=0,y=1\n", "-filterTags", "z=0", "y,x");
        test(rbb, "x=0,y=1,z=0\n", "-filterTags", "z=0", "y,x,z"); // you can output tags tag that is also in filterTags
        test(rbb, "y\tx\n0\t1\n1\t0\n", "-table", "y,x");

        rbb.disconnect();
    }

    private void test(RBB rbb, String expectedResult, String... args) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(bytes);
        SortedTagsets gt = new SortedTagsets(rbb);
        gt.parseArgs(args);
        gt.print(ps);
//        System.err.println("Wanted:\n" + expectedResult);
//        System.err.println("Got:\n" + bytes.toString());
        assertEquals(expectedResult, bytes.toString());
        bytes.close();
    }

}