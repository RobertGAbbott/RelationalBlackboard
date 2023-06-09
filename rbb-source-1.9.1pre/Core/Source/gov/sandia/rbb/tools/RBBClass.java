
package gov.sandia.rbb.tools;
import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBFilter;
import gov.sandia.rbb.Tagset;
import java.sql.SQLException;
import java.util.Map;

/**
 * RBBEntity provides a more object-oriented view of an RBB.
 *
 * RBB primarily takes a relational approach, where sets of related facts (RBB Events)
 * are linked by intersecting sets of tags.  There is no explicit representation
 * of groups of facts – i.e., objects.  The purpose of RBBClass is address this
 * need more conveniently and efficiently.
 *
 * This means doing the following conveniently and efficiently:
 * 1) Specify Classes (by identifying the relational links among RBB Events that constitute Objects)
 * 2) Discover "Objects" (sets of RBB Events that match specified criteria)
 * 3) Retrieve the value of an Object attribute, as of a specified time.
 *
 *
 * @author rgabbot
 */

public class RBBClass {

    /**
     * specifies which RBB Events can be each attribute
     *   this should perhaps just be in some particular constructor
     */
    Map<String, RBBFilter> attributeFilters;

    /*
     * code to extract non-persistent attributes
     */
    // Map<String, MLFeatureExtractor> attributeExtractors;

    /*
     * Impose constraints among tagsets of attributes.
     * <p>
     * name=value - all attributes have this name/value pair, e.g. platform=F18
     * name=      - all attributes have the same value for this tag, e.g. callsign=
     * name       - all attributes have this tag
     * 
     */
    Tagset idTags;

    class RBBObject {
        /*
         * non-persistent attributes have null ID
         */
        Map<String, Event> attributes;

        String getStringAttribute(Double time, String attributeName) {
            return null;
        }

        Float[] getTimeseriesAttribute(Double time, String attributeName) {
            return null;
        }

        byte[] getBlobAttribute(Double time, String attributeName) {
            return null;
        }
    }

    RBBObject[] findInstances(Double time) {
        return null;
    }

    RBBObject findInstance(Double time, String instanceTagValue) {
        return null;
    }
}
