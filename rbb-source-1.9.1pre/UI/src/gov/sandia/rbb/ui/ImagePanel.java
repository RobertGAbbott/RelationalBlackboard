/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ui;

import gov.sandia.rbb.RBB;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.tools.ImageEvent;
import gov.sandia.rbb.ui.images.ImageUtil;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Rectangle;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionAdapter;
import java.awt.image.BufferedImage;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.event.ChangeListener;
import replete.event.ChangeNotifier;
import replete.util.GUIUtil;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author dtrumbo
 */

public class ImagePanel extends JPanel {
    
    // FIELDS //
    private static final int MARGIN = 10;
    private ImageEvent[] events;
    private BufferedImage[] images;        
    private boolean[] imageHidden;        
    private Rectangle[] imageLocs;
    private Rectangle[] imageDelLocs;
    private int imgWidth;
    private int imgHeight;
    private int whichSelected = -1;
    private int whichHover = -1;
    private boolean deleteHover;

    /*
    - When enlarging/shrinking images, don’t change aspect ratio of them.  
      Center them in the display area.  Thus there will be empty space beside or 
      above/below each image (unless their aspect ratio permutationIsSubset that of the
      specified display width/height)
     */
    
    // CONSTRUCTOR //
    public ImagePanel(RBB rbb, Tagset ts, int imw, int imh) {
        imgWidth = imw;
        imgHeight = imh;
        try {
            events = ImageEvent.find(rbb, byTags(ts));
            images = new BufferedImage[events.length];
            for(int e = 0; e < events.length; e++) {
                images[e] = events[e].getImage();
            }
            imageHidden = new boolean[images.length];
        } catch(SQLException ex) {
            ex.printStackTrace();
        }
        
        addMouseMotionListener(new MouseMotionAdapter() {
            @Override
            public void mouseMoved(MouseEvent e) {
                whichHover = detectImage(e.getX(), e.getY());
                deleteHover = (detectImageDelete(e.getX(), e.getY()) != -1);
                repaint();
            }
        });
        addMouseListener(new MouseAdapter() {
            @Override
            public void mouseReleased(MouseEvent e) {
                int newSelected = detectImage(e.getX(), e.getY());
                if(newSelected != whichSelected) {
                    whichSelected = newSelected;
                    fireImageSelectedNotifier();
                }
                if(showDelete) {
                    int whichDelete = detectImageDelete(e.getX(), e.getY());
                    if(whichDelete != -1) {
                        fireImageDeleteClickedNotifier();
                    }
                }
                repaint();
            }
            @Override
            public void mouseExited(MouseEvent e) {
                whichHover = -1;
                repaint();
            }
        });
    }
    
    public ImageEvent getImageEvent(int index) {
        return events[index];
    }
    public int getSelectedImageIndex() {
        return whichSelected;
    }
    public void setSelectedImageIndex(int sel) {
        whichSelected = sel;
        repaint();
    }
    
    private int detectImage(int x, int y) {
        for(int r = 0; r < imageLocs.length; r++) {
            if(imageLocs[r] != null) {
                if(imageLocs[r].contains(x, y)) {
                    return r;
                }
            }
        }
        return -1;
    }
    private int detectImageDelete(int x, int y) {
        for(int r = 0; r < imageDelLocs.length; r++) {
            if(imageDelLocs[r] != null) {
                if(imageDelLocs[r].contains(x, y)) {
                    return r;
                }
            }
        }
        return -1;
    }

    // DELETE //
    private boolean showDelete = false;
    public boolean isShowDeleteIcon() {
        return showDelete;
    }
    public void setShowDeleteIcon(boolean sd) {
        showDelete = sd;
        repaint();
    }
    
    // LISTENERS //
    private ChangeNotifier imageSelectedNotifier = new ChangeNotifier(this);
    private void fireImageSelectedNotifier() {
        imageSelectedNotifier.fireStateChanged();
    }
    public void addImageSelectedListener(ChangeListener listener) {
        imageSelectedNotifier.addListener(listener);
    }
    private ChangeNotifier imageDeleteClickedNotifier = new ChangeNotifier(this);
    private void fireImageDeleteClickedNotifier() {
        imageDeleteClickedNotifier.fireStateChanged();
    }
    public void addImageDeleteClickedListener(ChangeListener listener) {
        imageDeleteClickedNotifier.addListener(listener);
    }
    
    public void setImageHidden(int index, boolean hidden) {
        imageHidden[index] = hidden;
        repaint();
    }

    // PAINT //
    @Override
    public void paintComponent(Graphics g) {
        super.paintComponent(g);

        imageLocs = new Rectangle[images.length];
        imageDelLocs = new Rectangle[images.length];
        List<Integer> shownImages = new ArrayList<Integer>();
        for(int i = 0; i < images.length; i++) {
            if(!imageHidden[i]) {
                shownImages.add(i);
            }
        }
        
        if(shownImages.size() == 0) {
            Dimension dim = new Dimension(getPreferredSize().width, 0);
            setPreferredSize(dim);
            return;
        }
        
        int pWidth = getWidth();
        int howMany = (pWidth - MARGIN) / (imgWidth + MARGIN);            
        int col = 0;
        int row = 0;
        for(int locIndex = 0; locIndex < shownImages.size(); locIndex++) {
            int imageIndex = shownImages.get(locIndex);
            
//        }
//        for(int i = 0; i < images.length; i++) {
            int drawX = MARGIN + (MARGIN + imgWidth) * col;
            int drawY = MARGIN + (MARGIN + imgHeight) * row;
//                BufferedImage scaled = new BufferedImage(imgWidth, imgHeight, images[i].getType());
//                scaled.getGraphics().drawImage(images[i], 0, 0, null);
//                g.drawImage(scaled, drawX, drawY, null);

            final float scaleX = (float) imgWidth / images[imageIndex].getWidth();
            final float scaleY = (float) imgHeight / images[imageIndex].getHeight();
            final float scale = scaleX < scaleY ? scaleX : scaleY;
            final float imgWidth0 = images[imageIndex].getWidth()*scale;
            final float imgHeight0 = images[imageIndex].getHeight()*scale;

            g.drawImage(images[imageIndex], 
                    (int)(drawX+(imgWidth-imgWidth0)/2.0f), (int)(drawY+(imgHeight-imgHeight0)/2.0f),
                    (int)imgWidth0, (int)imgHeight0,
                    null);

            imageLocs[imageIndex] = new Rectangle(drawX, drawY, imgWidth, imgHeight);
            if(whichSelected == imageIndex) {
                g.setColor(GUIUtil.deriveColor(Color.orange, -40, -40, -40));
                g.drawRect(drawX - 2, drawY - 2, imgWidth + 3, imgHeight + 3);
                g.setColor(Color.orange);
                g.drawRect(drawX - 1, drawY - 1, imgWidth + 1, imgHeight + 1);
                g.setColor(Color.yellow);
                g.drawRect(drawX, drawY, imgWidth - 1, imgHeight - 1);
            }
            if(whichHover == imageIndex) {
                g.setColor(new Color(40, 40, 40));
                g.drawRect(drawX - 2, drawY - 2, imgWidth + 3, imgHeight + 3);
                g.drawRect(drawX - 1, drawY - 1, imgWidth + 1, imgHeight + 1);
            }
            if(showDelete) {
                if(deleteHover && whichHover == imageIndex) {
                    g.setColor(Color.yellow);
                } else {
                    g.setColor(new Color(255, 255, 255, 130));
                }
                int delX = drawX + imgWidth - 20;
                int delY = drawY + imgHeight - 20;
                int delW = 20;
                int delH = 20;
                g.fillRect(delX, delY, delW, delH);
                ImageIcon trashIcon = ImageUtil.getImage("delete.gif");
                g.drawImage(trashIcon.getImage(), delX + 2, delY + 2, null);
                imageDelLocs[imageIndex] = new Rectangle(delX, delY, delW, delH);
            }

            // Stop loop early so we leave col and row what
            // they were on the last image painted.
            if(locIndex == shownImages.size() - 1) {
                break;
            }

            if(++col == howMany) {
                col = 0;
                row++;
            }
        }

        // Set just the height portion of the preferred size.
        int newY = MARGIN + (MARGIN + imgHeight) * (row + 1) - 1;
        Dimension dim = new Dimension(getPreferredSize().width, newY);
        setPreferredSize(dim);
    }
    
    /**
     * Show images from an RBB selected by a tagset.
     *
     * This is for calling UIMain from the command line - from java code call ImpagePanelMain instead.
     * If there is an exception, it prints out the message and aborts the program with an error code.
     */
    public static void main(String[] args) {
        try {
            ImagePanelMain(args);
        } catch(Throwable ex) {
            System.err.println(ex.getMessage());
            System.exit(-1);
        }
    }

    /**
     * unlike main(), ImagePanelMain() propagates exceptions so it is more useful for
     * calling from other code or unit tests.
     *
     * To call it from other code you will typically:
     * import static gov.sandia.rbb.ui.ImagePanel.ImagePanelMain
     *
     */
    public static void ImagePanelMain(String[] args) throws Throwable {
        String usage = "Usage: ImagePanel <dbURL> [tagset]";
        if(args.length == 0) {
            throw new Exception(usage);
        }

        RBB rbb = RBB.connect(args[0]);
        Tagset tags = null;
        if(args.length == 2) {
            tags = new Tagset(args[1]);
        }

        JFrame frame = new JFrame();
        frame.setTitle("Images");
        frame.add(new JScrollPane(new ImagePanel(rbb, tags, 100, 100)));
        frame.setSize(600, 600);
        frame.setLocationRelativeTo(null);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);

        rbb.disconnect();
    }
}
