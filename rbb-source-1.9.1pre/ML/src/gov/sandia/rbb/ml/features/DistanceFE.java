/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;


/**
 * <pre>
 * Computes the distance between pairs of inputs
 * or successive values of the same input (if the 1-input constructor is used)
 * Outputs null if either of these values is null
 *
 * Output:
 * y = a - b, the vector difference, if type = VECTOR
 * y = ||a-b||, the euclidian distance, if type = SCALAR
 * y = scalar distance between locations given by latitude/longitude in decimal degrees
 *
 *</pre>
 * @author rgabbot
 */
public class DistanceFE extends MLFeatureExtractor {

    public enum DistanceType { VECTOR, SCALAR, SCALAR_LAT_LON_M };

    private String inputA, inputB;
    DistanceType type;
    private Float[] prev;

    public DistanceFE(String output, String inputA, String inputB, DistanceType type, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, output);

        this.inputA = inputA;
        this.inputB = inputB;

        this.type = type;
    }

    public DistanceFE(String output, String inputA, DistanceType type, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, output);

        this.inputA = inputA;

        this.type = type;
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        Float[] a = obs.getFeatureAsFloats(inputA);

        Float[] b = prev;
        if(inputB != null)
            b = obs.getFeatureAsFloats(inputB);

        if(a==null || b==null) {
            // no op-do not set a result and accept default of null for feature value
        }
        else if(type == DistanceType.VECTOR) {
            obs.setFeatureAsFloats(this.getOutputName(0), vectorDifference(a,b));
        }
        else if(type == DistanceType.SCALAR) {
            obs.setFeatureAsFloats(this.getOutputName(0), (float) distance(a, b));
        }
        else if(type == DistanceType.SCALAR_LAT_LON_M) {
            obs.setFeatureAsFloats(this.getOutputName(0), (float) haversineDistance(a,b));
        }
        else {
            throw new IllegalArgumentException("DistanceFE.observe: invalid type of distance");
        }

        prev = a;

        super.observe(obs);
    }

    public static double distance(Float[] a, Float[] b) {
        return Math.sqrt(distSq(a,b));
    }

    public static double distSq(Float[] a, Float[] b) {
        double distSq = 0;
        for(int i = 0; i < a.length; ++i)
            distSq += Math.pow(b[i]-a[i], 2.0);
        return distSq;
    }

    /**
     * Return a new Float[] containing the vector difference, a-b
     */
    public static Float[] vectorDifference(Float[] a, Float[] b) {
        Float[] y = new Float[a.length];
        for(int i = 0; i < a.length; ++i)
            y[i] = a[i]-b[i];
        return y;
    }

    public static double haversineDistance(Float[] p1, Float[] p2) {
        final double R = 6371*1000; // radius of earth in m
        final double dLat = (p2[0]-p1[0]) * Math.PI/180.0;
        final double dLon = (p2[1]-p1[1]) * Math.PI/180.0;
        final double lat1 = p1[0] * Math.PI/180.0;
        final double lat2 = p2[0] * Math.PI/180.0;

        final double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.sin(dLon/2) * Math.sin(dLon/2) * Math.cos(lat1) * Math.cos(lat2);
        final double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c;
    }
}
