/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ui;

import gov.sandia.rbb.RBB;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.ui.images.ImageUtil;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Rectangle;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionAdapter;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.ImageIO;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.event.ChangeListener;
import replete.event.ChangeNotifier;
import replete.util.GUIUtil;

/**
 *
 * @author dtrumbo
 */

public class ImagePanelSearch extends JPanel {
    
    // FIELDS //
    private static final int MARGIN = 10;
    private SearchResult[] results;
    private Rectangle[] imageLocs;
    private Rectangle[] imageDelLocs;
    private int imgWidth;
    private int imgHeight;
    private int whichSelected = -1;
    private int whichHover = -1;
//    private boolean deleteHover;
    
    private static BufferedImage nonImage;
    
    static {
        try {
            nonImage = ImageIO.read(ImageUtil.class.getResource("unknown.jpg"));
        } catch (IOException ex) {
            Logger.getLogger(ImagePanelSearch.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /*
    - When enlarging/shrinking images, don’t change aspect ratio of them.  
      Center them in the display area.  Thus there will be empty space beside or 
      above/below each image (unless their aspect ratio permutationIsSubset that of the
      specified display width/height)
     */
    
    public void setResults(SearchResult[] newResults) {
        results = newResults;
        repaint();
    }
    
    // CONSTRUCTOR //
    public ImagePanelSearch(int imw, int imh) {
        imgWidth = imw;
        imgHeight = imh;
        results = new SearchResult[0];
        
        addMouseMotionListener(new MouseMotionAdapter() {
            @Override
            public void mouseMoved(MouseEvent e) {
                whichHover = detectImage(e.getX(), e.getY());
//                deleteHover = (detectImageDelete(e.getX(), e.getY()) != -1);
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
//                if(showDelete) {
//                    int whichDelete = detectImageDelete(e.getX(), e.getY());
//                    if(whichDelete != -1) {
//                        fireImageDeleteClickedNotifier();
//                    }
//                }
                if(e.getClickCount() > 1 && whichSelected != -1) {
                    fireImageDoubleClickedNotifier();
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
    
    public SearchResult getResult(int index) {
        return results[index];
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
    /*private int detectImageDelete(int x, int y) {
        for(int r = 0; r < imageDelLocs.length; r++) {
            if(imageDelLocs[r] != null) {
                if(imageDelLocs[r].contains(x, y)) {
                    return r;
                }
            }
        }
        return -1;
    }*/

    // DELETE //
    /*private boolean showDelete = false;
    public boolean isShowDeleteIcon() {
        return showDelete;
    }
    public void setShowDeleteIcon(boolean sd) {
        showDelete = sd;
        repaint();
    }*/
    
    // LISTENERS //
    private ChangeNotifier imageSelectedNotifier = new ChangeNotifier(this);
    private void fireImageSelectedNotifier() {
        imageSelectedNotifier.fireStateChanged();
    }
    public void addImageSelectedListener(ChangeListener listener) {
        imageSelectedNotifier.addListener(listener);
    }
    private ChangeNotifier imageDoubleClickedNotifier = new ChangeNotifier(this);
    private void fireImageDoubleClickedNotifier() {
        imageDoubleClickedNotifier.fireStateChanged();
    }
    public void addImageDoubleClickedListener(ChangeListener listener) {
        imageDoubleClickedNotifier.addListener(listener);
    }

    // PAINT //
    @Override
    public void paintComponent(Graphics g) {
        super.paintComponent(g);

        imageLocs = new Rectangle[results.length];
        imageDelLocs = new Rectangle[results.length];
        List<Integer> shownImages = new ArrayList<Integer>();
        for(int i = 0; i < results.length; i++) {
            shownImages.add(i);
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

            BufferedImage image = results[imageIndex].getImage();
            if(image == null) {
                image = nonImage;
            }
            int imWidth = image.getWidth();
            int imHeight = image.getHeight();
            
            final float scaleX = (float) imgWidth / imWidth;
            final float scaleY = (float) imgHeight / imHeight;
            final float scale = scaleX < scaleY ? scaleX : scaleY;
            final float imgWidth0 = imWidth * scale;
            final float imgHeight0 = imHeight * scale;

            g.drawImage(image, 
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
            /*if(showDelete) {
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
            }*/

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
        if(!dim.equals(getPreferredSize())) {
            setPreferredSize(dim);
            updateUI();
        }
        
    }
    
    /**
     * Show images from an RBB selected by a tagset.
     */
    public static void main(String[] args) {
        try {
            String usage = "Usage: ImangePanelSearch <dbURL> [tagset]";
            if(args.length == 0) {
                throw new Exception(usage + "\nMust specify an RBB!");
            }

            RBB rbb = RBB.connect(args[0]);
            Tagset tags = null;
            if(args.length == 2) {
                tags = new Tagset(args[1]);
            }

            JFrame frame = new JFrame();
            frame.setTitle("ImagePanel");
            frame.add(new JScrollPane(new ImagePanel(rbb, tags, 100, 100)));
            frame.setSize(600, 600);
            frame.setLocationRelativeTo(null);
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.setVisible(true);
            
            rbb.disconnect();
        } catch(Throwable ex) {
            ex.printStackTrace();
        }
    }
}
