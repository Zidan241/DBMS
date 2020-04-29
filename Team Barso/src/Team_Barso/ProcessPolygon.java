package Team_Barso;
import java.awt.Polygon;
import java.io.Serializable;
import java.util.Arrays;

public class ProcessPolygon  extends Polygon implements Comparable,Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -1825936098620236148L;


	/**
	 * 
	 */


	public int compareTo(Object p2) {
	
		return ((Double)(this.getBounds().getSize().getHeight()*this.getBounds().getSize().getWidth())).compareTo(((Polygon) p2).getBounds().getSize().getHeight()*((Polygon) p2).getBounds().getSize().getWidth());
	}
	public double calcArea() {
	return ((Double)(this.getBounds().getSize().getHeight()*this.getBounds().getSize().getWidth()));
	}

	
	public boolean equals(Object p) {
		return Arrays.equals(this.xpoints, ((ProcessPolygon) p).xpoints)&&Arrays.equals(this.ypoints,((ProcessPolygon)p).ypoints);
	}
	

}
