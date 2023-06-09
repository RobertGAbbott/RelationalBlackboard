/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;


/**
 * Compute the interior angle between two vectors in degrees.
 */
public class AngleBetweenVectorsFE extends MLFeatureExtractor {

    private String inputA, inputB;

    public AngleBetweenVectorsFE(String output, String inputA, String inputB, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, output);

        this.inputA = inputA;
        this.inputB = inputB;
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        Float[] a = obs.getFeatureAsFloats(inputA);
        Float[] b = obs.getFeatureAsFloats(inputB);

        if(a != null && b != null)
            obs.setFeatureAsFloats(this.getOutputName(0), angleBetweenVectors(a,b));

        super.observe(obs);
    }

    /*
     * Note: this will normalize the contents of a and b in place!
     */
    public static float angleBetweenVectors(Float[] a, Float[] b) {
        normalize(a);
        normalize(b);

        double dotProduct = 0;
        for(int i = 0; i < a.length; ++i)
            dotProduct += a[i] * b[i];

        float theta =  (float) Math.acos(dotProduct);

        if(Float.isNaN(theta)) {
            // sometimes theta is a little less than -1 or greater than 1 due to float inaccuracy.
            // System.err.println("AngleBetweenVectorsFE: failed to take acos of dotProduct="+dotProduct+" between vectors "+StringsWriter.join(",",a)+" and "+StringsWriter.join(",",b));
            if(dotProduct < -1.0f)
                theta = (float) Math.PI;
            if(dotProduct > 1)
                theta = 0.0f;
        }

        // convert to degrees.
        theta *= (180.0f / 3.14159f);

        return theta;
    }

    public static void normalize(Float[] a) {
        final float d = length(a);
        if(d==0)
            return;
        final float scale = 1.0f/d;
        for(int i = 0; i < a.length; ++i)
            a[i] *= scale;
    }

    public static float length(Float[] v) {
        double lengthSq = 0.0;
        for(int i = 0; i < v.length; ++i)
            lengthSq += v[i]*v[i];
        return (float) Math.sqrt(lengthSq);
    }

}
