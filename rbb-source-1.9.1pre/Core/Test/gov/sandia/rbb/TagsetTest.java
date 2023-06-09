/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb;

import java.util.Random;
import java.util.Iterator;
import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;
import static gov.sandia.rbb.Tagset.TC;

/**
 *
 * @author rgabbot
 */
public class TagsetTest {

    public TagsetTest() {
    }

    @Test
    public void testToTagsets() throws SQLException {
        Tagset[] a = Tagset.toTagsets("name=value,name2=value2");
        assertEquals(1, a.length);
        assertEquals(2, a[0].getNumTags());
        assertEquals("value", a[0].getValue("name"));
        assertEquals("value2", a[0].getValue("name2"));

        a = Tagset.toTagsets("name=value,name2=value2;name3=value3");
        assertEquals(2, a.length);
        assertEquals(2, a[0].getNumTags());
        assertEquals("value", a[0].getValue("name"));
        assertEquals("value2", a[0].getValue("name2"));
        assertEquals(1, a[1].getNumTags());
        assertEquals("value3", a[1].getValue("name3"));

        a = Tagset.toTagsets(new String[]{"name=value,name2=value2", "name3=value3"});
        assertEquals(2, a.length);
        assertEquals(2, a[0].getNumTags());
        assertEquals("value", a[0].getValue("name"));
        assertEquals("value2", a[0].getValue("name2"));
        assertEquals(1, a[1].getNumTags());
        assertEquals("value3", a[1].getValue("name3"));


        Tagset t = new Tagset("hi=there");
        a = Tagset.toTagsets(t);
        assertEquals(1, a.length);
        assertTrue(a[0]==t); // a[0] isn't a copy of t, it is t.

        Tagset t2 = new Tagset("ok");
        a = Tagset.toTagsets(new Object[]{t,t2});
        assertEquals(2, a.length);
        assertTrue(a[0]==t); // a[0] isn't a copy of t, it is t.
        assertTrue(a[1]==t2); // a[0] isn't a copy of t, it is t.

        assertNull(Tagset.toTagsets(null));
     }

    @Test
    public void testIsSubsetOf() throws SQLException {
        // empty tagset is subset of anything.
        assertTrue(TC("").isSubsetOf(TC("")));
        assertTrue(TC("").isSubsetOf(TC("a")));
        assertTrue(TC("").isSubsetOf(TC("a=1")));

        // nothing is a subset of empty tagset (except itself)
        assertFalse(TC("a").isSubsetOf(TC("")));
        assertFalse(TC("a=1").isSubsetOf(TC("")));

        // test null values
        assertTrue(TC("a").isSubsetOf(TC("a=1")));
        assertFalse(TC("a=1").isSubsetOf(TC("a")));

        // test multi-valued tags
        assertTrue(TC("a,a=1").isSubsetOf(TC("a=1")));
        assertTrue(TC("a=1").isSubsetOf(TC("a,a=1")));
        assertTrue(TC("a=1,a=2,b=3").isSubsetOf(TC("a=1,a=2,a=5,b=3,b=4")));
        assertFalse(TC("a=1,a=2,a=5,b=3,b=4").isSubsetOf(TC("a=1,a=2,b=3")));

        assertTrue(TC("a=1").isSubsetOf(TC("a=1,b=2")));
        assertFalse(TC("a=1,b=2").isSubsetOf(TC("a=1")));

    }

    @Test
    public void testHashCode() throws SQLException {
        Tagset t = TC("a=1,a=2");

        Tagset t2 = TC("a=1");
        t2.add("a", "2");

        assertEquals(t.hashCode(), t2.hashCode());

        Tagset t3 = t.clone();
        assertTrue(t.hashCode() == t3.hashCode());

        t3.set("x", null);
        assertTrue(t.hashCode() != t3.hashCode());

        Tagset t4 = t3.clone();
        assertEquals(t4.hashCode(), t3.hashCode());

        t3.set("x", "123");
        assertTrue(t4.hashCode() != t3.hashCode());

    }


    @Test
    public void testIntersection() throws SQLException {
        Tagset t = Tagset.intersection(Tagset.toTagsets(
                "a=1,a=2,b=3,c=4,z=1;"+
                "a=2,c=4,z=2;"+
                "z=3,c=4,a=2"
                ));
        assertEquals("a=2,c=4", t.toString());

        assertTrue(TC("a").contains("a", null));

        assertFalse(TC("a=1").contains("a", null)); // contains(s, null) is not a wildcard.

        t = Tagset.intersection(Tagset.toTagsets(
                "a=1,b=10;"+
                "a,b=11"
                ));
        assertEquals("a=1", t.toString());

        // same but with null value in first tagset.
        t = Tagset.intersection(Tagset.toTagsets(
                "a,b=11;"+
                "a=1,b=10"
                ));
        assertEquals("a=1", t.toString());

        t = Tagset.intersection(Tagset.toTagsets(
                "a,b=10;"+
                "a,b=11"
                ));
        assertEquals("a", t.toString());

        t = Tagset.intersection(Tagset.toTagsets(
                "a;"+
                "a=1,a=2,a=3;"+
                "a=2,a=3,a=4"
                ));
        assertEquals("a=2,a=3", t.toString());

        t = Tagset.intersection(Tagset.toTagsets(
                "a;"+
                "a=1,a=2,a=3,a;"+
                "a=2,a=3,a=4"
                ));
        assertEquals("a=2,a=3,a=4", t.toString());


    }

    @Test public void testTemplate() throws SQLException {
        assertEquals(TC("a=1,b=2"), Tagset.template(TC("a=1,b"), TC("a=10,b=2,c=11")));

        // return value will still contain a null value if the content didn't have a value
        assertEquals(TC("a,b=2"), Tagset.template(TC("a,b"), TC("b=2,c=11")));

        ////// the rest are obscure tests about null values and multi-valued tags.

        // return value will still contain a null value if the content had a null value
        assertEquals(TC("a,b=2"), Tagset.template(TC("a,b"), TC("a,b=2,c=11")));

        // content may specify multiple values
        assertEquals(TC("a=91,a=92,b=2"), Tagset.template(TC("a,b"), TC("a=91,a=92,b=2,c=11")));

        // if the template has both a non-null value and a null vlaue, the null value will still be replaced by the value(s) specified by the content.
        assertEquals(TC("a=1,a=92,a=93,b=2"), Tagset.template(TC("a=1,a,b"), TC("a=92,a=93,b=2,c=11")));

    }


    @Test
    public void testPermutationIsSubset() throws SQLException
    {
        {
        Tagset[] t = new Tagset[] { TC("a=1"), TC("b=2"), TC("c=3")};
        Tagset[] T = new Tagset[] { TC("a=1"), TC("b=2"), TC("c=3")};
        Tagset[] p = Tagset.permutationIsSuperset(T, t);
        assertTrue(p!=null);
        System.err.println(toString(p) + " matches " + toString(T));
        }

        {
        Tagset[] t = new Tagset[] { TC("a=1"), TC("b=2"), TC("c=3,d=4")};
        Tagset[] T = new Tagset[] { TC("a=1"), TC("b=2"), TC("c=3")};
        Tagset[] p = Tagset.permutationIsSuperset(T, t);
        assertTrue(p==null);
        }

        {
        Tagset[] t = new Tagset[] { TC("a=1"), TC("b=2"), TC("c=3")};
        Tagset[] T = new Tagset[] { TC("a=1"), TC("b=2"), TC("c=3,d=4")};
        Tagset[] p = Tagset.permutationIsSuperset(T, t);
        assertTrue(p!=null);
        System.err.println(toString(p) + " matches " + toString(T));
        }

        {
        Tagset[] t = new Tagset[] { TC("a=1"), TC("b=2"), TC("c=3")};
        Tagset[] T = new Tagset[] { TC("c=3,d=4"), TC("a=1"), TC("b=2")};
        Tagset[] p = Tagset.permutationIsSuperset(T, t);
        assertTrue(p!=null);
        System.err.println(toString(p) + " matches " + toString(T));
        }

        {
        Tagset[] t = new Tagset[] { TC("a=1"), TC("b=2"), TC("d=4,c=3")};
        Tagset[] T = new Tagset[] { TC("c=3,d=4"), TC("a=1,aa=11"), TC("bb=22,b=2")};
        Tagset[] p = Tagset.permutationIsSuperset(T, t);
        assertTrue(p!=null);
        System.err.println(toString(p) + " matches " + toString(T));
        }

        {
        Tagset[] t = new Tagset[] { TC("a=1"), TC("b=2"), TC("cc=33,d=4,c=3")};
        Tagset[] T = new Tagset[] { TC("c=3,d=4"), TC("a=1,aa=11"), TC("bb=22,b=2")};
        Tagset[] p = Tagset.permutationIsSuperset(T, t);
        assertTrue(p==null);
        }

        {
        Tagset[] t = new Tagset[] { TC("a=1"), TC("b=2"), TC("cc=33,d=4,c=3")};
        Tagset[] T = new Tagset[] { TC("c=3,d=4"), TC("a=1,aa=11"), TC("bb=22,b=2")};
        Tagset[] p = Tagset.permutationIsSuperset(T, t);
        assertTrue(p==null);
        }

        {
        Tagset[] t = new Tagset[] { TC("a"), TC("b=2"), TC("c=3")};
        Tagset[] T = new Tagset[] { TC("a=1"), TC("b=2"), TC("c=3")};
        Tagset[] p = Tagset.permutationIsSuperset(T, t);
        assertTrue(p!=null);
        System.err.println(toString(p) + " is superset of " + toString(t));
        }

        {
        Tagset[] t = new Tagset[] { TC("a=1"), TC("b=2"), TC("c=3")};
        Tagset[] T = new Tagset[] { TC("a"), TC("b=2"), TC("c=3")};
        Tagset[] p = Tagset.permutationIsSuperset(T, t);
        assertTrue(p==null);
        }

        {
        Tagset[] t = new Tagset[] { TC("a"), TC("b=2"), TC("c=3")};
        Tagset[] T = new Tagset[] { TC("x"), TC("a=1"), TC("q"), TC("b=2"), TC("c=3")};
        Tagset[] p = Tagset.permutationIsSuperset(T, t);
        assertTrue(p!=null);
        System.err.println(toString(p) + " is superset of " + toString(t));
        }

        {
        Tagset[] t = new Tagset[] { TC("a=1"), TC("b=2"), TC("c=3")};
        Tagset[] T = new Tagset[] { TC("a=1,b=2"), TC("c=3,b=2"), TC("b=2,c=4") };
        Tagset[] p = Tagset.permutationIsSuperset(T, t);
        assertTrue(p!=null);
        System.err.println(toString(p) + " is superset of " + toString(t));
        }

    }


    String toString(Tagset[] t)
    {
        String result = t[0].toString();
        for(int i = 1; i < t.length; ++i)
            result += " " + t[i].toString();
        return result;
    }

   @Test
    public void testAdd() throws java.sql.SQLException {
//        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
//        System.err.println("Entering "+methodName);

        Tagset t = new Tagset("b=2");
        assertEquals("b=2", t.toString());

        t.add(new Tagset("a=1"));
        assertEquals("a=1,b=2", t.toString());

        // try adding multiple values for a name at once.
        t.add(new Tagset("b=22,b=222"));
        assertEquals("a=1,b=2,b=22,b=222", t.toString());

   }

   static String remove(String tagset, String name) throws java.sql.SQLException {
       Tagset t = new Tagset(tagset);
       t.remove(name);
       return t.toString();
   }

   static String remove(String tagset, String name, String value) throws java.sql.SQLException {
       Tagset t = new Tagset(tagset);
       t.remove(name, value);
       return t.toString();
   }

   @Test
    public void testNulls() throws java.sql.SQLException {

   }

   @Test
    public void testRemove() throws java.sql.SQLException {

       // remove name when it wasn't present anyways
       assertEquals("a=1,b=2", remove("a=1,b=2", "z"));

       // remove name when it was present.
       assertEquals("a=1,a=2", remove("a=1,a=2,b=2,b=3", "b"));

       // remove name/value when neither name/value was present
       assertEquals("a=1,b=2", remove("a=1,b=2", "z","3"));

       // remove name/value when name was present but value was not
       assertEquals("a=1,b=2", remove("a=1,b=2", "b","3"));

       // remove name/value when they were present and there were no other values for that name
       assertEquals("b=2", remove("a=1,b=2", "a","1"));

       // remove name/value when they were present and there were other values for that name
       assertEquals("a=3,b=2", remove("a=1,b=2,a=3", "a","1"));

       // remove null value
       Tagset t = new Tagset("a=1,c=3,b,b=2");
       assertEquals("a=1,b,b=2,c=3", t.toString());
       t.remove("b",null);
       assertEquals("a=1,b=2,c=3", t.toString());
   }

    /**
     * Test of main method, of class Tagset.
     */
    @Test
    public void testFunctionality()
        throws Exception
    {
        String s = "a=1,b=2,c=3,d=4,d=44,b=22";
        Tagset t = new Tagset(s);

        System.out.print("The names in " + s + " are: ");
        for (Iterator<String> i = t.getNames().iterator(); i.hasNext();)
        {
            System.out.print(i.next());
        }
        System.out.println("");

        System.out.print("The values of b are: ");
        for (Iterator<String> i = t.getValues("b").iterator(); i.hasNext();)
        {
            System.out.print(i.next());
            if (i.hasNext())
            {
                System.out.print(",");
            }
        }
        System.out.println("");

        System.out.println("The value of c is: " + t.getValue("c"));

        t.add("c", "33");
        System.out.println("after adding c=33, now the tagset is: "
            + t.toString());

        String s3 = "3";
        s3 += "3";
        t.add("c", s3);
        System.out.println("after adding c=33 again, now the tagset is: "
            + t.toString());

        t.remove("c", "3");
        System.out.println("after removing c=3, now the tagset is: "
            + t.toString());

        t.remove("b");
        System.out.println("after removing all b, now the tagset is: "
            + t.toString());

        gov.sandia.rbb.Tagset t2 = t.clone();

        t.set("d", "444");
        System.out.println("after setting d=444, now the tagset is: "
            + t.toString());


        System.out.println("tagset before setting d=444: " + t2.toString());


        reportIsSubsetOf(new Tagset(), new Tagset());
        reportIsSubsetOf(new Tagset("a=1,b=2"), new Tagset("b=2,a=1"));
        reportIsSubsetOf(new Tagset("a=1,b=2,c=3"), new Tagset("a=1,b=2"));
        reportIsSubsetOf(new Tagset("a=1,b=2,b=3,b=4"), new Tagset(
            "b=2,a=1,b=3"));
        reportIsSubsetOf(new Tagset("a=1,b=2,b=3,b=4"), new Tagset(
            "b=2,a=1,b=5,b=3"));


        assertFalse(new Tagset("a=1,a=2,b=3").equals(new Tagset("a=1,b=3")));
        assertFalse(new Tagset("a=1,b=3").equals(new Tagset("a=1,a=2,b=3")));
    }

    static private void reportIsSubsetOf(gov.sandia.rbb.Tagset t,
        gov.sandia.rbb.Tagset t2)
    {
        System.out.println(t.toString() + (t.isSubsetOf(t2) ? " is" : " is not")
            + " a subset of " + t2.toString());
        System.out.println(t2.toString()
            + (t2.isSubsetOf(t) ? " is" : " is not") + " a subset of "
            + t.toString());
    }

    public static String makeRandomString(Random r, int minLength, int maxLength) {
        StringBuilder sb = new StringBuilder();
        int len = r.nextInt(maxLength-minLength+1)+minLength;
        for(int i = 0; i < len; ++i)
            sb.append((char)r.nextInt(55296));

        //characters above 55296 are NOT valid unicode characters.

        return sb.toString();
    }

    public static Tagset makeRandomTagset(Random r) {
        Tagset t = new Tagset();

        int numTags = r.nextInt(5);

        for(int iTag=0; iTag < numTags; ++iTag)
        {
            String name = makeRandomString(r, 1, 5);
            String value = r.nextInt(4)==0 ? null : makeRandomString(r, 0, 5); // null and empty values are allowable
            t.add(name,value);
        }

        return t;
    }

    private void printStringChars(String s) {
        for(int i = 0; i < s.length(); ++i) {
            if(i>0)
                System.err.print(" ");
            System.err.print((int) s.charAt(i));
        }
        System.err.println(""); // newline
    }

    @Test
    public void testToStringFromString()
        throws Exception
    {
        System.err.println("testToStringFromString()");

        Random r = new Random();

        for(int iTagset = 0; iTagset < 100000; ++iTagset) {
            final Tagset before = makeRandomTagset(r);
            final Tagset after = new Tagset(before.toString());

            assertEquals(before, after);
        }
    }
}