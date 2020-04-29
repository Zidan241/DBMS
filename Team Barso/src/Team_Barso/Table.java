package Team_Barso;
import java.awt.Polygon;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Vector;

public class Table implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6443330443225532050L;
	// transient?
	int pages=0;
	boolean ClusterIndexed=false;
	int indicies;
	String strTableName;
	String strClusteringKeyColumn;
	Hashtable<String, String> htblColNameType;
	ArrayList<Object[]> Ranges=new ArrayList<Object[]>();
	
	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType2) {
		
		
		this.strTableName = strTableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType2;
		indicies=0;

	}

	public void DisplayTable() {
		for (int j = 1; j <= this.pages; j++) {

			Page p = (Page) DBApp.DeSerialize("data/" + this.strTableName + j + ".ser");
			Object[] MinMax=this.Ranges.get(j-1);
			
			if (MinMax[0] instanceof Polygon) {
				System.out
						.println("Minimum area in page " + j + " is :" + ((Polygon) MinMax[0]).getBounds().getSize().height
								* ((Polygon) MinMax[0]).getBounds().getSize().width);
				System.out
						.println("Maximum area in page " + j + " is :" + ((Polygon) MinMax[1]).getBounds().getSize().height
								* ((Polygon)MinMax[1]).getBounds().getSize().width);

			} else {
				System.out.println("Minimum of page " + j + " is : " + MinMax[0]);
				System.out.println("Maximum of page " + j + " is : " + MinMax[1]);

			}

			for (int i = 0; i < p.arrRecords.size(); i++) {

				p.arrRecords.get(i).forEach((k, v) -> {
					if (v instanceof Polygon) {
						System.out.print(k + "= {");

						for (int t = 0; t < ((Polygon) v).xpoints.length; t++) {
							System.out.print("(" + ((Polygon) v).xpoints[t] + "," + ((Polygon) v).ypoints[t] + ")");
							if (t < ((Polygon) v).xpoints.length - 1)
								System.out.print(",");
						}
						System.out.print("} ");
					}

					else if (v instanceof Date) {
						Date date=(Date)v;
						String[] arr=date.toString().split(" ");
						String year =arr[5];
					System.out.print(k + "=" + v + " ");

				//		System.out.print(k+" = "+year + "/" + (1+date.getMonth()) + "/" + date.getDay()+" ");
					}

					else
						System.out.print(k + "=" + v + " ");

				});
				System.out.println("");

			}
		}
	}

}
