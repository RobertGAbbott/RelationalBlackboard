/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;

/**
 *
 * @author rgabbot
 */
public class H2SProblem {

    /**
     * This method supports the SQL alias RBB_DISTANCE.
     */
    public static Float distance(Object[] x, Object[] y) {
        float d = 0;
        for(int i = 0; i < x.length; ++i)
        {
            final float d0 = toFloat(x[i]) - toFloat(y[i]);
            d += d0*d0;
        }
        return (float) java.lang.Math.sqrt(d);
    }

    public static float toFloat(Object x) {
        return x instanceof Float ? (Float) x : Float.parseFloat(x.toString());
    }
}
