/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservation.Metadata;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Set;


/**
 * Creates a feature which is the position of the member of the group that
 * is nearest the reference point.
 *
 * When the group is empty, the output is set to null.
 *
 * @author rgabbot
 */
public class NearestFE extends MLFeatureExtractor {

    private String position, velocity, group;
    private MLObservation.Metadata metadata;
    private String[] prevNearest;
    private boolean split;

    /**
     * @param output: name of the created feature, e.g. "nearestEnemy"
     * @param position: position of the reference point, e.g. "myPosition"
     * @param velocity: vector velocity of reference point, e.g. "myVelocity".  If non-null, the 'nearest' is computed via Closest Point of Approach, with the "nearest" being a 'reasonable' combination of time to CPA and salience at CPA.  If velocity is null, the simple salience is used.
     * @param group: name of the group input, e.g. "enemyPositions"
     * @param numOutputs: return nearest N.  For all n > 1 the created feature is 'output'-n, e.g. "nearestEnemy-2" and so on.
     * @param if split is true, the timeseries will be split every time the identity of the nearest member of the group changes.
     * @param nextFeatureExtractor
     */
    public NearestFE(String output, String position, String velocity, String group, int numOutputs, boolean split, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, numberedOutputs(output, numOutputs));

        this.position = position;
        this.velocity = velocity;
        this.group = group;
        this.split = split;
    }

    private static String[] numberedOutputs(String name, int n) {
        if(n==1)
            return new String[]{name};
        
        String[] result = new String[n];
        for(int i = 0; i < n; ++i)
            result[i] = name + "-" + (i+1);
        // System.err.println("Outputs are: "+StringsWriter.join(",", result));

        return result;
    }

    @Override
    public void init(double time, Metadata md) throws Exception {
        super.init(time, md);
        metadata = md;
        prevNearest = null;
    }

    @Override
    public void observe(MLObservation obs) throws Exception {

        String[] nearestInGroup = nearest(obs, position, velocity, group, null, null, getNumOutputs());

        if(split && prevNearest != null && !Arrays.equals(prevNearest, nearestInGroup)) {
            super.done(obs.getTime());
            super.init(obs.getTime(), metadata);
        }

        int i = 0;
        for(String member : nearestInGroup)
            obs.setFeature(getOutputName(i++), obs.getFeatureAsGroup(group).getFeatureAsFloats(member));
        while(i < getNumOutputs())
            obs.setFeature(getOutputName(i++), null);

        prevNearest = nearestInGroup;

        super.observe(obs);
    }

    private static class DistanceToMember implements Comparable {
        DistanceToMember(String member, Double distance) {
            this.distance = distance;
            this.member = member;
        }

        Double distance;
        String member;

        @Override
        public int compareTo(Object o) {
            DistanceToMember a = this;
            DistanceToMember b = (DistanceToMember) o;

            return b.distance.compareTo(a.distance);
        }
    }

    /**
     * Returns the index within the group that indentifies the point nearest position.
     */
    public static String[] nearest(MLObservation obs, String position, String velocity, String groupName, Set<String> exclude, Float maxDistance, int numOutputs) {
        Float[] a = obs.getFeatureAsFloats(position);
        Float[] v = velocity==null ? null : obs.getFeatureAsFloats(velocity);
        MLObservation group = obs.getFeatureAsGroup(groupName);

        PriorityQueue<DistanceToMember> q = new PriorityQueue<DistanceToMember>(numOutputs);

        for(String groupMember : obs.getFeatureAsGroup(groupName).getFeatureNames()) {
            if(exclude!=null && exclude.contains(groupMember))
                continue;

            Float[] b = group.getFeatureAsFloats(groupMember);
            Double d = salience(a, v, b);
            if(d!=null)
                q.add(new DistanceToMember(groupMember, d));

            while((q.size() > numOutputs) ||
                    (maxDistance!=null && q.size()>0&&q.peek().distance > maxDistance))
                q.poll();
        }

        String[] result = new String[q.size()];
        for(int i = 0; i < result.length; ++i)
            result[result.length-i-1] = q.poll().member; // q returns results from worst to best.

        return result;
    }

    private static Double salience(Float[] a, Float[] v, Float[] b) {
        if(v == null)
            return DistanceFE.distance(a, b);
        else {
            ClosestPointOfApproachFE.Result cpa = ClosestPointOfApproachFE.CPA(a, v, b, null, true);

            if(cpa.timeUntilCPA < 0)
                cpa.timeUntilCPA = -cpa.timeUntilCPA * 2.0f; // allow it to be closest, but penalize it.

            return Math.pow(cpa.timeUntilCPA*35,2)+Math.pow(cpa.distanceAtCPA,2);
        }
    }

}
