/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.examples.util;

/**
 *
 */
public class LLA2ECEF
{

    private final double a = 6378137; // radius

    private final double e = 8.1819190842622e-2;  // eccentricity

    private final double esq = Math.pow(e, 2);

    private double[] lla2ecef(double[] lla)
    {
        double lat = lla[0];
        double lon = lla[1];
        double alt = lla[2];

        double N = a / Math.sqrt(1 - esq * Math.pow(Math.sin(lat), 2));

        double x = (N + alt) * Math.cos(lat) * Math.cos(lon);
        double y = (N + alt) * Math.cos(lat) * Math.sin(lon);
        double z = ((1 - esq) * N + alt) * Math.sin(lat);

        double[] ret =
        {
            x, y, z
        };
        return ret;
    }

}
