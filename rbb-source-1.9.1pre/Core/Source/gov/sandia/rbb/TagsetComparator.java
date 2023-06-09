/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * Compares pairs of tagsets, placing an ordering on them.
 * This is most often used to sort tags for display.
 *<p>
 * Can compare tag values as numbers - see compareStringSets
 *<p>
 * The order in which to consider tags can be specified - see sortBy
 *<p>
 * To compare Events (or Timeseries) see CompareEventsByTags
 *<p>
 *
 * @author rgabbot
 */
public class TagsetComparator implements Comparator<Tagset> {

    /**
     * Neither a nor b may be null, although tag values in either may be null
     */
    @Override
    public int compare(Tagset a, Tagset b)
    {
        return direction * compareAscending(a,b);
    }

    /*
     * Compare values of a specific tag as numbers.
     *<p>
     * Comparision will raise a NumberFormatException if this is not used correctly.
     */
    public void compareAsNumbers(String tagName) { compareAsNumbers.add(tagName); }

    /**
     * Calls compareAsNumbers on all tags whose values are all numbers.
     *<p>
     * Specifically, any tag for which a parseDouble successfully converts a value,
     * and fails on no other value of the tag in any tagset, will be treated as a number.
     * A null value for a tag does not count as either a success or a failure.
     *<p>
     * This can either be called on the same array of Tagsets that are going to be sorted,
     * or on a smaller array (even a single tagset) whose value types are representative of the
     * data to be sorted.
     *<p>
     * If you are also going to call sortBy(), call it first, so only those tags are checked.
     */
    public void compareNumbersAsNumbers(Tagset... tags) {
        Set<String> bad = new HashSet<String>();
        for(Tagset t : tags) {
            NAMES: for(String name : _sortBy == null ? t.getNames() : _sortBy) {
                if(bad.contains(name))
                    continue NAMES; // once conversion fails for a tag name once it is banned forever.
                for(String value : t.getValues(name)) {
                    if(value == null)
                        continue;
                    try {
                        Double.parseDouble(value);
                        compareAsNumbers.add(name);
                    }
                    catch(NumberFormatException e) {
                        compareAsNumbers.remove(name);
                        bad.add(name);
                        continue NAMES;
                    }
                }
            }
        }
        // System.err.println("Comparing tag values as numeric for tags: "+StringsWriter.join(" ", compareAsNumbers.toArray()));
    }

    /**
     * tag names can be added to this set; if so, all values for the tag are compared
     * as numbers and an exception is raised if any value (other than null) is not
     * convertible to a double.
     */
    final Set<String> compareAsNumbers = new HashSet<String>();

    /**
     *
     * Specify the order in which to compare tags in each tagset pair.
     * Only the specified tags are used in the ordering;
     * Tagsets that differ only in un-named tags will be considered equal.
     *
     */
    public void sortBy(String... tagNames) {
        _sortBy = Arrays.asList(tagNames);
    }

    /**
     * Sort in descending order, rather than default of ascending.
     */
    public void sortDescending() {
        direction = -1;
    }

    /**
     * ascending = 1, descending = -1
     */
    private int direction = 1;

    private Collection<String> _sortBy = null;

    private int compareAscending(Tagset a, Tagset b) {
        Collection<String> sortBy = _sortBy;
        if(sortBy == null) { // no specified sort tagnames - compare by all.
            int nameComparison = compareStringSets(a.getNames(), b.getNames());
            if(nameComparison != 0)
                return nameComparison;
            sortBy = a.getNames(); // tagNames are equal, so a.getNames().equals(b.getNames())
        }

        for(String name : sortBy) {
            int valueComparison;
            if(compareAsNumbers.contains(name))
                valueComparison = compareNumberStringSets(a.getValues(name), b.getValues(name));
            else
                valueComparison = compareStringSets(a.getValues(name), b.getValues(name));
            if(valueComparison != 0)
                return valueComparison;
        }

        return 0; // identical.
    }

    private <T> int compareArrays(T[] a, T[] b, Comparator<T> c) {
        Arrays.sort(a, c);
        Arrays.sort(b, c);
        for(int i = 0; i < a.length && i < b.length; ++i)
            if(c.compare(a[i], b[i]) != 0)
                return c.compare(a[i], b[i]);
        // they are equal up to the shorter of the two.
        if(a.length != b.length)
            return a.length - b.length; // whoever has fewer is first.

        return 0;
    }

    Comparator<String> compareStrings = new CompareValues<String>();
    int compareStringSets(Set<String> a, Set<String> b) {
        if(a==null || b ==null)
            return compareToNull(a,b);
        return compareArrays(a.toArray(new String[0]), b.toArray(new String[0]), compareStrings);
    }

    Comparator<Double> compareDoubles = new CompareValues<Double>();
    int compareNumberStringSets(Set<String> a, Set<String> b) {
        if(a==null || b==null)
            return compareToNull(a,b);
        return compareArrays(toDoubles(a), toDoubles(b), compareDoubles);
    }

    private Double[] toDoubles(Set<String> s) {
        Double[] d = new Double[s.size()];
        int i = 0;
        for(String s0 : s)
            d[i++] = s0 == null ? null : Double.parseDouble(s0);
        return d;
    }


    /**
     * Compare two comparable things, allowing either or both to be null.
     */
    static class CompareValues<T extends Comparable<? super T>> implements Comparator<T> {
        @Override
        public int compare(T a, T b) {
            if(a==null || b==null)
                return compareToNull(a,b);
            return a.compareTo(b);
        }
    }

    private static int compareToNull(Object a, Object b) {
        if(a == null) {
            if(b==null)
                return 0;
            return -1;
        }
        if(b == null)
            return 1;
        throw new IllegalArgumentException("TagsetComparator error: either a or b must be null");
    }
}
