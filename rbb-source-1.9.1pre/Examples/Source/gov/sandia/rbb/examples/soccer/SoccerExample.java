/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.examples.soccer;

import gov.sandia.rbb.ui.DrawTimeseries;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 *
 * jdbc:h2:tcp://localhost//Users/rgabbot/workspace/Spatial/RelationalBlackboard/test;ifexists=true;DB_CLOSE_DELAY=-1

 * @author rgabbot
 */
public class SoccerExample {
    public static void main(String[] args) {
      try {
          SoccerExampleMain(args);
        }
        catch (Throwable ex)
        {
            System.err.println("SoccerExample Exception: " + ex.toString());
            System.exit(-1);
        }
    }

    public static void SoccerExampleMain(String[] args) throws Throwable {
        String path = "/gov/sandia/rbb/examples/soccer/";

        ArrayList<String> drawArgs = new ArrayList<String>(Arrays.asList(
            // "-server", do not add -server option here, the demo web page adds it as a parameter
            "-background", path+"SoccerField.png",

            "-maxTime", "6000", // half_time=300, 2x halves, 10x cycles per.
            "-filterTags", "Select A Game",

            "-speed", "10",

            "-filterTagsMulti", "game,type=position",
            "-filterTagsMulti", "game,side,type=position",
            "-filterTags", "id=Ball",

            "-label", "id",
            
            "-icon", "side=L", path+"LPlayer.png",
            "-icon", "side=R", path+"RPlayer.png",
            "-icon", "id=Ball", path+"Ball.png",
            "-icon", "id=Pass", path+"PinkBall.png",

            "-noDots"));

            drawArgs.addAll(Arrays.asList(args));

        DrawTimeseries draw = new DrawTimeseries(drawArgs.toArray(new String[]{}));
        
    }
}
