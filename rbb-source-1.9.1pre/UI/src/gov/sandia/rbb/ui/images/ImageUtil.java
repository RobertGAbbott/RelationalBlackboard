package gov.sandia.rbb.ui.images;

import javax.swing.ImageIcon;
import replete.util.GUIUtil;

public class ImageUtil {
    public static ImageIcon getImage(String name) {
        return GUIUtil.getImageLocal(name);
    }
}
