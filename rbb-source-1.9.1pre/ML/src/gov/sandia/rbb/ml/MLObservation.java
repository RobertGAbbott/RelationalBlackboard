/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.util.StringsWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An MLObservation is an ordered list of named Features at a moment in time.
 */
public class MLObservation implements Comparable<MLObservation> {
    Metadata metadata; // package visibility
    private Double time;
    private Object[] featureData;

    public MLObservation(double time, Metadata metadata) {
        this.time = time;
        this.metadata = metadata;
        if(metadata != null)
            this.featureData = new Object[metadata.size()];
    }

    // make an observation and set the metadata at the same time.
    public MLObservation(double time, Metadata featureNames, Object... features) {
        if(featureNames.size() < features.length)
            throw new IllegalArgumentException("Observation constructor got values for "+features.length+" features but the specified FeatureNames has only "+featureNames.size());
        this.time = time;
        this.metadata = featureNames;
        this.featureData = new Object[featureNames.size()];
        for(int i = 0; i < features.length; ++i)
            this.featureData[i] = features[i];
    }

    public Double getTime() { return this.time; };

    public int getNumFeatures() {
        return metadata.size();
    }

   /**
    * Retrieve the data for the specified feature.
     * Throws IllegalArgumentException if there is no feature with the name FeatureName
     */
     public Object getFeature(String featureName) {
        return featureData[metadata.getFeatureIndex(featureName)];
    }

   /**
    * Retrieve the RBB Event (which may also be a Timeseries) associated with the Feature,
    * which will be null unless this is an Input Feature (i.e. a sample
    * interpolated directly from the Timeseries)
    * Throws IllegalArgumentException if there is no feature with the name FeatureName
    */
     public Event getFeatureEvent(String featureName) {
        return metadata.getFeatureEvent(featureName);
    }

    /**
     * Throws IllegalArgumentException if there is no feature with the name FeatureName.
     * Returns null if no value has been set for the feature in this Observation, or
     * a value has been set but is not and cannot be converted to a Float[]
     */
    public Float[] getFeatureAsFloats(String featureName) {
        Object o = getFeature(featureName);
        if(o==null)
            return null; // no value has been set for this feature.

        if(o instanceof Float[]) // yay, it already was a Float[]
            return (Float[]) o;

        if(o instanceof Object[]) {
            Object[] a = (Object[]) o;
            Float[] floats = new Float[a.length];
            for(int i = 0; i < floats.length; ++i)
                floats[i] = Float.parseFloat(a[i].toString());
            return floats;
            
        }

        return null;
    }

   /**
     * Throws IllegalArgumentException if there is no feature with the name FeatureName -
     * the feature must already be listed in the metadata.
     */
     public void setFeature(String featureName, Object feature) {
        featureData[metadata.getFeatureIndex(featureName)] = feature;
    }

   /**
     * Throws IllegalArgumentException if there is no feature with the name FeatureName -
     * the feature must already be listed in the metadata.
     */
    public void setFeatureAsFloats(String featureName, Float... features) {
        setFeature(featureName, (Object)features);
    }

    /**
     * This is used to iterate over the features in this observation.
     */
    public Set<String> getFeatureNames() {
        return metadata.getFeatureNames();
    }

    private Object getFeature(int featureIndex) {
        return featureData[featureIndex];
    }

    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        StringsWriter s = new StringsWriter();
        s.write("t="+getTime().toString());
        for(String featureName : getFeatureNames()) {
            s.writeStrings(" ", featureName, "=");
            Object feature = getFeature(featureName);
            if(feature==null)
                s.writeStrings("null");
            else if(feature instanceof String)
                s.write(feature.toString());
            else if(feature instanceof Object[])
                s.writeJoin(",", (Object[])feature);
            else if(feature instanceof GroupFeature)
                s.writeStrings("(", ((GroupFeature)feature).toString(), ")");
            else
                throw new RuntimeException("MLObservation - a feature has type "+feature.getClass().getName()+" which toString doesn't know how to handle.");
        }
        return s.toString();
    }

    /**
     * compare on the basis of time.
     */
    public int compareTo(MLObservation o) {
        return (int) Math.signum(time - o.time);
    }

    /**
     * 
     */
    public static class GroupFeature extends MLObservation {
        GroupFeature(Double time, Metadata metadata) {
            super(time, metadata);
        }
    }

    /**
     * Returns null if no value has been set for the feature in this Observation.
     * Throws IllegalArgumentException if there is no feature with the name FeatureName,
     * or if a feature has been set but it is not an instance of GroupFeature
     * 
     */
      public GroupFeature getFeatureAsGroup(String featureName) {
        Object feature = getFeature(featureName);
        if(feature==null)
            return null;
        if(feature instanceof GroupFeature)
            return (GroupFeature) feature;

        throw new IllegalArgumentException("MLObservation - tried to access feature "+featureName+" as a GroupFeature, but it's actually a "+feature.getClass().getName());
    }

    public void promoteGroupFeature(String groupName, String groupMemberID, String newFeatureName) {
        setFeature(newFeatureName, getFeatureAsGroup(groupName).getFeature(groupMemberID));
    }

//    public String getNameOfGroupFeature(String groupName, int index) {
//        return getFeatureAsGroup(groupName).getFeatureNames().getFeatureName(index);
//    }

    /**
     * Metadata captures information about the metadata in a sequence of Observation.
     * By definition the metadata does not change within a sequence of observations.
     * Thus it is more efficient for the entire sequence to share 1 copy of the metadata
     * rather than every Observation repeating it.
     * This keeps the MLObservation instances very simple - they just have an
     * Object[] of the feature data itself.
     *
     */
    public static class Metadata implements Cloneable {

        private Map<String, Integer> featureIndex;
        private ArrayList<String> featureNames;
        private ArrayList<Event> featureEvents;

        public Metadata() {
            featureIndex = new java.util.HashMap<String, Integer>();
            featureNames = new ArrayList<String>();
            featureEvents = new ArrayList<Event>();
        }

        public int size() {
            return this.featureIndex.size();
        }

        public Set<String> getFeatureNames() {
            return featureIndex.keySet();
        }

        public void add(String name, Event event) {
            if(this.featureIndex.get(name) != null)
                throw new IllegalArgumentException("Attempt to add duplicate feature to FeatureNames: "+name);
            this.featureIndex.put(name, this.featureNames.size());
            this.featureNames.add(name);
            this.featureEvents.add(event);
        }

        @Override
        public String toString() {
            StringWriter sw = new StringWriter();
            sw.write("Feature Set: ");
            if(featureNames.size() >= 1)
                sw.write(featureNames.get(0));
            for(int i = 1; i < featureNames.size(); ++i) {
                sw.write(",");
                sw.write(featureNames.get(i));
            }
            sw.write(" Input Events:");
            for(Event e : featureEvents)
                sw.write(" "+e);
            return sw.toString();
        }

       /**
         * Throws IllegalArgumentException if there is no feature with the name FeatureName
         */
        int getFeatureIndex(String name) { // package visibility
            Integer result = this.featureIndex.get(name);
            if(result==null)
                throw new IllegalArgumentException("MLObservation.Metadata.getFeatureIndex: no such feature: "+name);
            return result;
        }

       /**
         * Throws IllegalArgumentException if there is no feature with the name FeatureName
         */
        public Event getFeatureEvent(String name) {
            return featureEvents.get(getFeatureIndex(name));
        }

        /*
         * return any/all Events attached to features (with no nulls, but the result may be of length 0)
         */
        public Event[] getAllFeatureEvents() {
            ArrayList<Event> result = new ArrayList<Event>();
            for(Event e : featureEvents)
                if(e!=null)
                    result.add(e);
            return result.toArray(new Event[0]);
        }

       /**
         * Throws IllegalArgumentException if there is no feature with the name FeatureName
         */
        public void setFeatureEvent(String name, Event event) {
            featureEvents.set(getFeatureIndex(name), event);
        }

        @Override
        public Metadata clone() {
            Metadata md = new Metadata();
            for(String featureName : getFeatureNames())
                md.add(featureName, this.getFeatureEvent(featureName));
            return md;
        }


        /**
         *
         * <pre>
         *
         * This function is a mechanism for a "child" tagset to inherit tag names and values
         * from the tagsets of one or more "parent" inputs in this Metadata instance.
         *
         * Substitution is triggered if the child contains a tag:
         * {tagName}={inputName}.{inputTagName}
         * the entire value is replaced with the value of the specified tag belonging to the input.
         *
         * The {tagName} may contain an asterisk, which is replaced with the name of the tag being inherited.
         * (If the tag name is nothing but the asterisk, the tag name is the same as in the parent.)
         *
         * The {inputTagName} may also be the wildcard (asterisk):
         * {tagName}={inputName}.*
         * All the tags of the specified input are inherited, except for tags the child already had.
         * (If you do want to inherit an additional value for a tag, use a non-wildcard to get it explicitly, see example 7 below.)
         * When inheriting multiple tags, an asterisk is normally used in the {tagName} so the inherited tags don't all have the same name.
         *
         * Finally, the reserved string RBBID can be used as an inputTagName.
         * It is replaced with the RBB ID of the input.  In this case t's tagName cannot contain a wildcard.
         *
         * Examples:
         * Assume this observation has an input with the tagset:
         * wingman: side=good,name=HanSolo,species=human
         *
         * 1) myside=wingman.side               -> myside=good
         * 2) *=wingman.side                    -> side=good
         * 3) *=wingman.*                       -> side=good,species=human,name=HanSolo
         * 4) *=wingman.*,name=Luke             -> side=good,species=human,name=Luke
         * 5) name=Luke,MyWingman.*=wingman.*   -> name=Luke,MyWingman.side=good,MyWingMan.name=HanSolo,MyWinMan.species=human
         * 6) name=Luke,wingmanID=wingman.RBBID -> name=Luke,wingmanID=123
         * 7) name=Luke,name=wingman.name       -> name=Luke,name=HanSolo
         *
         * These examples are taken from MLObservationTest, so if any examples need
         * to be added to resolve ambiguities, an example and test case should both be added.
         *
         * If no substitutions are made t is returned unchanged.
         * Throws an exception if there is an error retrieving a tag value from t
         *
         * </pre>
        */
        public Tagset tagValueSubstitution(Tagset t) throws Exception {
            // wildcards are done second so they won't take precedence over non-wildcard tags.
            return tagValueSubstitution(tagValueSubstitution(t, false), true);
        }

        /**
         * if wildcard is true, will only do substitutions ending in .* (meaning all tags are inherited from the input).
         * if wildcard is false, substitutions ending in .* are not done.
         */
        private Tagset tagValueSubstitution(Tagset t, boolean wildcard) throws Exception {
            Tagset t2=null; // result.  Allocate only if necessary.
            Pattern namePat = Pattern.compile("(.*?)\\*(.*?)");
            Pattern valuePat = Pattern.compile("(.*)\\.(.*)");

            for(String tagName: t.getNames()) {
                for(String tagValue : t.getValues(tagName)) {
                    if(tagValue == null)
                        continue;
                    Matcher valueMatch = valuePat.matcher(tagValue);
                    if(!valueMatch.matches())
                        continue;
                    final String inputName = valueMatch.group(1);
                    final String inputTagName = valueMatch.group(2);
//                    System.err.println(tagValue + " " + valueMatch.groupCount() + " " + inputName + " " + inputTagName);
                    Event inputEvent = getFeatureEvent(inputName);
                    if(inputEvent==null)
                        continue;

                    // implement RBBID
                    if(inputTagName.equals("RBBID")) {
                        if(t2==null)
                            t2 = new Tagset(t);
                        t2.remove(tagName, tagValue);
                        t2.add(tagName, inputEvent.getID().toString());
                    }

                    // the name of the tag(s) we will create has three parts: <prefix><*><postfix>
                    Matcher nameMatch = namePat.matcher(tagName);

                    for(String inputTagName0 : inputEvent.getTagset().getNames()) {
                        if(wildcard && !inputTagName.equals("*"))
                            continue; // only doing wildcards right now, and this isn't one.
                        if(!wildcard && !inputTagName.equals(inputTagName0))
                            continue; // this is not the specific tag we're looking for right now.

                        // determine the name of the tag we will now create.
                        String name = tagName;
                        if(nameMatch.matches()) // wildcard in tag name means to use the name of the tag from the parent.
                            name = nameMatch.group(1) + inputTagName0 + nameMatch.group(2);

                        Set<String> inputTagValues = inputEvent.getTagset().getValues(inputTagName0);

                        if(wildcard && t.containsName(name))
                            continue; // wildcards don't take precedence.

                        if(t2==null)
                            t2 = new Tagset(t);

                        t2.remove(tagName, tagValue); // remove the pattern, e.g. inputName.tagName or inputName.*

                        for(String inputTagValue : inputTagValues)
                            t2.add(name, inputTagValue);
                    }
                }
            }

            if(t2!=null)
                return t2;
            return t;
        }
    }
}
