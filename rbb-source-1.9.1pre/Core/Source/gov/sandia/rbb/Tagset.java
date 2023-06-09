
package gov.sandia.rbb;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;


/**
 * <pre>
 * An RBB Tagset is a set of name=value pairs.
 * A Tagset can be represented either as a string x1=y1,x2=y2...
 * or as an instance of this class.
 *
 * </pre>
 * @author rgabbot
 */

public class Tagset implements Cloneable
{

    private Map<String, Set<String>> _pairs;

    private Map<String, Set<String>> allocate_names()
    {
         // use TreeMap instead of e.g. HashMap so names will be in sorted order
        return new java.util.TreeMap<String, Set<String>>();
    }

    private Set<String> allocate_values()
    {
        // use TreeMap instead of e.g. HashMap so names will be in sorted order.
        // Values (unlike names) may be null, so the custom StringComparatorNullOK is used.
        return new TreeSet<String>(new StringComparatorNullOK());
    }

    public Tagset()
    {
    }

    /**
     * <pre>
     * Creates a Tagset from:
     *
     * A tagset in a string, e.g. "name1=value1,name2=value2"
     * An array with alternating name/value pairs, e.g. (name1,value1, name2,value2)
     * A java.sql.Array with alternating names/values (name1,value1, name2,value2)
     * An existing tagset instance (makes a deep copy)
     *
     * o cannot be null.
     * </pre>
     */
    public Tagset(Tagset t) {
        add(t);

//        if(o instanceof Tagset)
//            add((Tagset)o);
//        else if(o instanceof String)
//
//
//        // convert whatever it was to an Object[]
//
//        try {
//
//        Object[] a;
//        if(o instanceof Object[])
//            a = (Object[]) o;
//        else if(o instanceof java.sql.Array)
//            a = (Object[]) ((java.sql.Array)o).getArray();
//        else
//            a = H2STagset.fromString(o.toString());
//
//        // now it's an array.
//        for(int i = 0; i < a.length; i+=2)
//            add(a[i].toString(), (a[i+1]==null ? null : a[i+1].toString()));
//
//        } catch(java.sql.SQLException e) {
//            System.err.println("Tagset(Object o) Error converting java.sql.Array to Tagset: "+e);
//        }
    }


    /*
     * Convert a Tagset from the String representation "name1=value1,name2,name3="
     * to the Object[] representation { "name1","value1", "name2",null, "name3","" }
     * Tags are delimited by ','
     * If the tag contains one or more '=' characters, everything before the first '='
     * is the name, and everything after it is the value.
     * If the tag doesn't contain '=', the entire string is the name, and the value is null.
    */
    public Tagset(String s) {
        if(s==null || s.equals(""))
            return; // empty strings are valid, but split(",") returns an array with 1 element, not 0, so the normal-path code is incorrect.

        for(String pair : s.split(",")) {
            String[] nameValue = pair.split("=", 2);
            String name = decode(nameValue[0]);
            String value = null;
            if(nameValue.length > 1)
                value = decode(nameValue[1]);
            add(name, value);
        }
    }

    /**
     * Create a tagset from an array of alternating name/value pairs
     */
     public Tagset(Object[] tags) {
        for(int i = 0; i < tags.length; i+=2)
            add(tags[i].toString(), i+1<tags.length && tags[i+1] != null ? tags[i+1].toString() : null);
     }


//    public Tagset(Object... tags) {
//        fromArray(tags);
//    }

//    public Tagset(Tagset tags) {
//        fromArray(tags.toArray());
//    }

    /**
     *
     * syntactic sugar for allocating a Tagset.
     * use:
     * import static gov.sandia.rbb.Tagset.TC;
     *
     */
    public static Tagset TC(String tags) {
        return new Tagset(tags);
    }

    /**
     * Create an array of Tagset instances.
     * Each element of tagSets must itself be an Object[], which represents a single tagset.
     *<p>
     * tagSets may also be a 1d array - in this case an array of 1 Tagset is returned
     * This is to accomodate H2, which fails to recognize single-element arrays as arrays.
     * (H2 docs say adding an extra comma to the single-element array works, but it doesn't).
     */
//    public static Tagset[] multiFromArray(Object[] tagSets)
//    {
//        if(tagSets.length==0) {
//            return new Tagset[0];
//        }
//        else if(tagSets.length > 0 && tagSets[0] instanceof Object[]) {
//            Tagset[] t = new Tagset[tagSets.length];
//            for (int i = 0; i < tagSets.length; ++i)
//                t[i] = new Tagset((Object[]) tagSets[i]);
//            return t;
//        }
//        else
//        {
//            return new Tagset[] { new Tagset(tagSets) };
//        }
//    }

//    public void fromArray(Object[] tags) {
//        removeAll();
//        for(int i = 0; i < tags.length; i+=2) {
//            add(tags[i].toString(), (tags[i+1]==null ? null : tags[i+1].toString()));
//        }
//    }
//
    /**
     * Convert to an array of Strings.
     * a[2*i] is the name, a[2*i+1] is the value.
     */
    public Object[] toArray()
    {
        Object[] a = new Object[getNumTags()*2];
        int i = 0;
        for(Iterator<String> nameIter = getNames().iterator(); nameIter.hasNext(); ) {
            final String name = nameIter.next();
            for(Iterator<String> valueIter = getValues(name).iterator(); valueIter.hasNext(); ) {
                a[i] = name;
                ++i;
                a[i] = valueIter.next();
                ++i;
            }
        }
        return a;
    }

//    public static Object[][] multiToArray(Tagset[] tagsets) {
//        Object[][] result = new Object[tagsets.length][];
//        for(int i = 0; i < tagsets.length; ++i)
//            result[i] = tagsets[i].toArray();
//        return result;
//    }

    @Override
    public Tagset clone()
    {
        Tagset t = new Tagset();
        t.add(this);

        return t;
    }

    public Set<String> getNames()
    {
        if (_pairs == null)
        {
            _pairs = allocate_names();
        }
        return _pairs.keySet();
    }

    /**
     * Return the number of name=value pairs in this tagset.
     */
    public int getNumTags()
    {
        int n = 0;
        if(_pairs == null) {
            return n;
        }
        for(Iterator<Set<String>> i = _pairs.values().iterator(); i.hasNext(); ) {
            n += i.next().size();
        }
        return n;
    }

    /**
     * get the values associated with the name, or null if none.
     */
    public Set<String> getValues(String name)
    {
        if (_pairs == null)
        {
            return null;
        }
        return _pairs.get(name);
    }

    public String getValue(String name)
    {
        Set<String> values = getValues(name);
        if (values == null)
        {
            return null;
        }
        Iterator<String> iter = values.iterator();
        if (!iter.hasNext())
        {
            return null;
        }
        return iter.next();
    }

   /**
    *
    * determine whether the name=value pairs in this tagset are a a subset of those in superSet.
    *<p>
    * Any null tag values in this tagset are considered a subset of any tagset in superSet with the same tag name -
    * in other words, a null value is a wildcard.
    *
    */
    public boolean isSubsetOf(Tagset superSet)
    {
        for (String subsetName : getNames())
        {
            Set<String> superSetValues = superSet.getValues(subsetName);
            if (superSetValues == null)
                return false; // this has a name that the would-be superset does not.

            for(String subsetValue : this.getValues(subsetName)) {
                if(subsetValue == null)
                    continue; // if a subset tag has a null value it permutationIsSubset any tag in the superset with the same name.
                if (!superSetValues.contains(subsetValue))
                    return false;
            }
        }

        // we didn'superSet find anything we had that superSet did not, so we're a subset of superSet.
        return true;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null)
            return false;
        if (o == this)
            return true;
        if (!(o instanceof Tagset))
            return false;
        Tagset b = (Tagset) o;
        Tagset a = this;

        if(a.getNames().size() != b.getNames().size())
            return false;

        for(String name : a.getNames()) {
            if(!b.containsName(name))
                return false;
            Set<String> aValues = a.getValues(name);
            Set<String> bValues = b.getValues(name);
            if(aValues.size() != bValues.size())
                return false;
            for(String value : aValues)
                if(!bValues.contains(value))
                    return false;
        }
        
        return true;
    }

    @Override
    public int hashCode()
    {
        // return _pairs.hashCode(); // does a shallow hash with lots of collisions!
        return toString().hashCode();
    }

    /** add a name/value pair to this tagset, even if another pair already had the same name   */
    public void add(String name, String value)
    {
        Set<String> values = getValues(name);
        if (values == null)
        {
            values = allocate_values();
            if (_pairs == null)
                _pairs = allocate_names();
            _pairs.put(name, values);
        }
        values.add(value);
    }

    public void add(Tagset t)
    {
        for (String name : t.getNames())
            for(String value : t.getValues(name))
                add(name, value);
    }

    /**
     * remove any/all pairs with the specified name, then add the name/value pair
     * @param name
     * @param value
     */
    public void set(String name, String value)
    {
        remove(name);
        add(name, value);
    }

    /**
     * remove any/all pairs with names in newTags, then add newTags
     * Does not remove any existing names not overwritten by newTags.
     *
     * @param newTags
     */
    public void set(Tagset newTags)
    {
        for (String name : newTags.getNames())
            remove(name);

        add(newTags);
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        for(String name : getNames()) {
            for(String value : getValues(name)) {
                if(sb.length() > 0)
                    sb.append(",");
                sb.append(encode(name));
                if(value == null)
                    continue;
                sb.append('=');
                sb.append(encode(value));
            }
        }
        return sb.toString();
    }

    public static String[] toStrings(Tagset[] a) {
        String[] s = new String[a.length];
        for(int i=0; i < a.length; ++i)
           s[i] = a.toString();
        return s;
    }

    /*
     * <pre>
     * Creates an array of Tagsets from:
     * 
     * A tagset in a string, e.g. "a=b"
     * Multiple tagsets in a string, e.g. "a=b;c=d"
     * An array of strings each with a tagset, e.g. ("a=b", "c=d")
     * A Tagset instance or array of Tagset instances (then it's a fast no-op)
     * 
     * if o is null, null is returned.
     *
     * for examples see TagsetTest.testToTagsets()
     * </pre>
     */
    public static Tagset[] toTagsets(Object o) {
        if(o == null)
            return null;

        if(o instanceof Tagset)
            return new Tagset[]{(Tagset)o};

        if(o instanceof Tagset[])
            return (Tagset[]) o;

        if(o instanceof Object[]) {
            Object[] a = (Object[]) o;
            if(a.length == 0)
                return new Tagset[0];
            Tagset[] tags = new Tagset[a.length];
            for(int i = 0; i < a.length; ++i) {
                if(a[i] instanceof  Tagset)
                    tags[i] = (Tagset) a[i];
                else
                    tags[i] = new Tagset(a[i].toString());
            }
            return tags;
        }
        else {
            // otherwise assume the string representation of o is one or more tagsets.
            String[] a = o.toString().split(";", -1); // -1 means that "a=b;" translates to two tagsets, the second being empty.
            Tagset[] tags = new Tagset[a.length];
            for(int i = 0; i < a.length; ++i)
                tags[i] = new Tagset(a[i]);
            return tags;
        }
    }

//    /*
//     * <pre>
//     * Creates a Tagset from:
//     *
//     * A tagset in a string, e.g. "a=b"
//     * An array of name/value pairs, e.g. (a,b)
//     * An existing tagset instance (then it's just a passthrough - no copy is created)
//     *
//     * if o is null, null is returned.
//     * </pre>
//     */
//    public Tagset toTagset(Object o) {
//
//    }

//    /** set name / value pairs from a string in the format:
//    name1=value1,name2=value2...
//    Any previous name/value pairs are lost */
//    public void fromString(String tagset) {
//        fromArray(H2STagset.fromString(tagset));
//    }

    /**
     * Construct an array of Tagset from a string.
     *
     * Tagsets in the string are separated by a semicolon.
     *
     * Tagset[] tags = multiFromString("name=a,n=1;name=b,n=2")
     *
     */
//    static public Tagset[] multiFromString(String s) {
//        String[] a = s.split(";");
//        Tagset[] result = new Tagset[a.length];
//        for(int i = 0; i < a.length; ++i)
//            result[i] = new Tagset(a[i]);
//        return result;
//    }

    static public String multiToString(Tagset[] tsa) {
        StringBuilder sb = new StringBuilder();
        for(int i =  0; i < tsa.length; ++i) {
            if(i > 0)
                sb.append(";");
            sb.append(tsa[i].toString());
        }
        return sb.toString();
    }

    /**
     *
     * Returns a copy of the template with any null values replaced by the value for the same tag from a.
     *
     * For example:
     * template(TC("a=1,b"), TC("a=10,b=2,c=11")) = TC("a=1,b=2")
     * template(TC("a,b"), TC("b=2,c=11")) = TC("a,b=2")
     *
     * The result will still contain a null value if the content doesn't have the corresponding tag, or has a null value for it.
     *
     * @param t
     * @param a
     * @return
     */
    public static Tagset template(Tagset template, Tagset content) {
        Tagset result = new Tagset(template);
        for(String name : template.getNames())
            if(result.contains(name, null)) {
                if(!content.containsName(name))
                    continue; // no corresponding tag in the content = no change.
                result.remove(name, null);
                for(String value : content.getValues(name))
                  result.add(name, value);
            }
        return result;
    }

    /**
     *
     * Construct a Tagset that is the intersection of other Tagsets
     *
     * A null-valued tag is a wildcard, in that the result will contain the value
     * from the non-null-valued tag.
     * e.g. the intersection of (a,b=10) and (a=1,b=11) is (a=1)
     * If all tags have a null value for a name the so does the result:
     * Note the intersection of (a,b=10) and (a,b=11) is (a)
     *
     *
     *
     */
    public static Tagset intersection(Tagset... tagsets) {
        if(tagsets == null)
            return null;
        Tagset result = new Tagset();
        if(tagsets.length == 0)
            return result;

        // the basic approach is to loop over all the name/value pairs in tagsets[0],
        // and return any if its name/value pairs that also belong to all other tagsets.

        NAMES: for (String name : tagsets[0].getNames())
        VALUES: for (String value : tagsets[0].getValues(name)) // for every name/value pair in tagsets[0]...
        {
            for (int i = 1; i < tagsets.length; ++i)
            {
                Set<String> values = tagsets[i].getValues(name);
                if(values == null)
                    continue NAMES; // one of the tagsets doesn't even have this name, so there's no value for it in the intersection.
                else if(tagsets[i].contains(name, value)) {
                    // continue on, this is he happy path.
                    // this includes the case where tagsets[0] and tagsets[0] both have null value.
                }
                else if(tagsets[i].contains(name, null)) {
                    // continue on -
                    // tagsets[i] has a null value (but tagsets[0] must not, or it would have matched the contains(name,value).)
                }
                else if(value == null) {
                    // tagsets[0] has a null value (but tagsets[i] must not, or it would have matched the contains(name,value).)
                    // This means tagsets[0] doesn't care what the value is but tagsets[i] does.
                    // make tagsets[0] adopt the values of tagsets[i] and restart.
                    Tagset restart = new Tagset(tagsets[0]);
                    restart.remove(name);
                    restart._pairs.put(name, values);
                    tagsets = Arrays.copyOf(tagsets, tagsets.length);
                    tagsets[0] = restart;
                    return intersection(tagsets);
                }
                else {
                    // tagsets[i] doesn't have this value, and neither tagsets[0] nor tagsets[i] has a null, so exclude this value.
                    continue VALUES;
                }
            }
            // The name/value didn't fail a match with any of the tagsets, so it's in the intersection.
            result.add(name, value);
        }
        return result;
    }

    public void removeAll() {
        if(_pairs != null) {
            _pairs.clear();
        }
    }


    /** remove any/all name/value pairs with the specified name
     */
    public void remove(String name)
    {
        if (_pairs != null)
        {
            _pairs.remove(name);
        }
    }

    /** remove any/all name/value pairs with the specified name and value.
     */
    public void remove(String name, String value)
    {
        if (_pairs == null)
            return;

        Set<String> values = getValues(name);
        if(values == null)
            return;

        values.remove(value);

        if(values.size() == 0)
            _pairs.remove(name);
    }

    public boolean containsName(String tagName)
    {
        return(getNames().contains(tagName));
    }

    /*
     * Returns true iff any of the names in the set have the specified value.
     */
    public boolean containsValue(String value) {
        for(String name : getNames())
            if(contains(name, value))
                return true;
        return false;
    }

   /**
    * Returns p, a permutation of T such that t[i].isSubsetOf(T[i]) is true for all i,
    * for i between 0 and t.length.
    *
    * The first such permutation encountered is returned immediately without determining if others exist.
    *
    * null is returned if no permutation of T is a pairwise superset of t.
    *
    * T (the superset) may be longer than t in which case the result will have
    * the length of t and some elements of T will not be used.
    *
    * This function actually has nothing to do with Tagset in particular
    * and belongs in a Tagset utility class.
    */
   public static Tagset[] permutationIsSuperset(Tagset[] T, Tagset[] t)
   {
       if(T.length < t.length)
           return null;

       if(t.length == 0)
           return new Tagset[0];

       // create our working array.
       Tagset[] p = new Tagset[T.length];
       for(int i = 0; i < p.length; ++i)
           p[i] = T[i];

       return permutationIsSupersetHelper(p, t, 0);
   }

   private static Tagset[] permutationIsSupersetHelper(Tagset[] T, Tagset[] t, int start)
    {
       for(int i = start; i < T.length; ++i)
       {
           swap(T, start, i);
           if(t[start].isSubsetOf(T[start]))
           {
               if(start == t.length-1)
                   return Arrays.copyOf(T, t.length);
               Tagset[] p = permutationIsSupersetHelper(T, t, start+1);
               if(p!=null)
                   return p;
           }
           swap(T, start, i);
       }
       return null;
   }

   private static void swap(Tagset[] t, int i1, int i2)
   {
    Tagset t0 = t[i1];
    t[i1] = t[i2];
    t[i2] = t0;
   }

    public boolean contains(String name, String value)
    {
        Set<String> values = this.getValues(name);
        if(values == null)
            return false;
        return values.contains(value);
    }


    /**
     * Make a new Tagset that is the union of srcTags, then override with values from
     * changeTags using set().  Any null-valued tags in changeTags are removed.
     */
    public static Tagset deriveTags(Tagset changeTags, Tagset... srcTags)
    {
        Tagset result = new Tagset();

        for (int i = 0; i < srcTags.length; ++i)
            result.add(srcTags[i]);


        if (changeTags != null) {
            for(String tagName : changeTags.getNames()) {
                result.remove(tagName);
                for(String tagValue : changeTags.getValues(tagName)) {
                    if(tagValue != null)
                        result.add(tagName, tagValue);
                }
            }
        }

        return result;
    }


    /**
     *<pre>
     * Escape characters that are used by the RBB code for parsing the
     * string representation of a Tagset (or RBBFilter).
     *
     * Note this method assumes the String consists only of valid Unicode characters.
     * For example, new String(byte[]) does NOT ensure this, so putting arbitrary
     * things like images into a tag will not necessarily work.
     *
     * http://stackoverflow.com/questions/1536054/how-to-convert-byte-array-to-string-and-vice-versa
     *
     * URLEncoding is performed only if the string contains one of:
     *
     * =    delimiter between name/value
     * ,    delimiter between name/value pairs in a tagset
     * \t   (tab) delimiter between fields in H2EventTCPServer / H2EventTCPClient
     * ;    delimiter between tagsets in multiFromString
     * :    delimeter between parameters to feature extractor in RBBML, and RBBFilter fields.
     * \n   (newline) H2EventTCPClient / H2EventTCPServer use line-oriented input.
     *
     * Finally,
     * %    percent must be encoded since that is the escape char for URL encoding itself.
     * +    also for URL encoding.
     *
     * </pre>
     */
    public static String encode(String s)
    {
        boolean needsEncoding = false;
        for(int c : delimeters) {
            if(s.indexOf(c) >= 0) {
                needsEncoding = true;
                break;
            }
        }

        if(!needsEncoding)
            return s;

        try
        {
            s = URLEncoder.encode(s, "UTF-8");
            s = s.replaceAll("%2F", "/");
            return s;
        }
        catch (UnsupportedEncodingException ex)
        {
            // cannot happen, since the encoding is hard-coded, and valid.
            return null;
        }

    }

    private static final int[] delimeters = new int[]{'%', '=', ',', '\t', '\n', ' ', ':', ';', '+'};

    /**
     * Inverse of encode(String)
     */
    public static String decode(String s)
    {
        try
        {
            return URLDecoder.decode(s, "UTF-8");
        }
        catch (UnsupportedEncodingException ex)
        {
            // cannot happen, since the encoding is hard-coded, and valid.
            return null;
        }

    }
}

class StringComparatorNullOK implements java.util.Comparator<String>
{
   @Override
   public int compare(String o1, String o2)
   {
       if(o1 == null) {
           if(o2 == null)
               return 0;
           return -1;
       }
       if(o2 == null)
           return 1;
       return o1.compareTo(o2);
   }

}
