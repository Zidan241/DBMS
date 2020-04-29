package Team_Barso;

import java.awt.Point;
import java.awt.Polygon;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import Team_Barso.BPlusTree.InternalNode;
import Team_Barso.BPlusTree.LeafNode;
import Team_Barso.BPlusTree.Node;
import Team_Barso.BPlusTree.RangePolicy;

public class DBApp implements Serializable {
	// The data folder is generated automatically

	ArrayList<String[]> Meta = new ArrayList<String[]>();
	ArrayList<String> Tables = new ArrayList<String>();
	ArrayList<String> Names = new ArrayList<String>();

	public void init() throws IOException, FileNotFoundException {
		// reading data from metadata.csv and writing it into the Meta arraylist
		// reading names from a csv files
		File data = new File("data");
		data.mkdir();
		File config = new File("config");
		config.mkdir();

		File metadata = new File("data/metadata.csv");

		if (metadata.exists()) {

			String s = "data/metadata.csv";
			BufferedReader br = new BufferedReader(new FileReader(s));
			try {
				br = new BufferedReader(new FileReader(s));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			String line;
			try {
				int i = -1;

				while ((line = br.readLine()) != null) {

					if (i >= 0) {
						Meta.add(line.split(","));

					}
					i++;
				}

			} catch (IOException e) {
				e.printStackTrace();

			}

		}

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		try {
			boolean dup = false;
			for (int j = 0; j < Meta.size(); j++) {
				if (Meta.get(j)[0].equals(strTableName)) {
					throw (new DBAppException("There is already a table with name" + strTableName));
				}

			}
			Tables.add(strTableName);
			htblColNameType.put("TouchDate", String.class.getCanonicalName());
			Table tblCreatedTable = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
			serialize("data" + "/" + strTableName + ".ser", tblCreatedTable);

			// Serialize Table

			System.out.println();
			// Write into Metadata.csv
			Object[] arrKeys = htblColNameType.keySet().toArray();
			Object[] arrValues = htblColNameType.values().toArray();

			FileWriter csvWriter;
			File metadata = new File("data/metadata.csv");
			if (metadata.exists()) {
				csvWriter = new FileWriter("data/metadata.csv", true);
			} else {
				csvWriter = new FileWriter("data/metadata.csv", true);
				csvWriter.append("Table Name, Column Name, Column Type, ClusteringKey, Indexed\n");
			}

			for (int i = 0; i < htblColNameType.size(); i++) {
				boolean boolClusteringKey = false;
				System.out.println(arrKeys[i] + " , " + arrValues[i]);
				csvWriter.append(strTableName);
				csvWriter.append(",");
				csvWriter.append((String) arrKeys[i]);
				csvWriter.append(",");
				csvWriter.append((String) arrValues[i]);
				csvWriter.append(",");

				if (arrKeys[i].equals(strClusteringKeyColumn)) {
					csvWriter.append("true");
					boolClusteringKey = true;
				} else {
					csvWriter.append("false");
				}

				csvWriter.append(",");

				// Indexing
				csvWriter.append("false");

				csvWriter.append("\n");
				if (boolClusteringKey) {
					String[] column = { strTableName, ((String) arrKeys[i]), ((String) arrValues[i]), "true", "false" };
					Meta.add(column);
				} else {

					String[] column = { strTableName, ((String) arrKeys[i]), ((String) arrValues[i]), "false",
							"false" };
					Meta.add(column);
				}

			}

			csvWriter.flush();
			csvWriter.close();
		} catch (Exception i) {
			i.printStackTrace();
		}

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			Properties prop = new Properties();

			prop.load(new FileInputStream("config/DBApp.properties"));

			int X = Integer.parseInt(prop.getProperty("X"));
			int N = Integer.parseInt(prop.getProperty("N"));

			int MetaIndex = -1;
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
			LocalDateTime now = LocalDateTime.now();

			LinkedHashMap<String, Object> htblColNameValue1 = new LinkedHashMap<String, Object>();
			htblColNameValue.put("TouchDate", dtf.format(now));
			htblColNameValue.forEach((k, v) -> {

				if (!(v instanceof Polygon))
					htblColNameValue1.put(k, v);
				else {
					ProcessPolygon add = new ProcessPolygon();
					for (int i = 0; i < ((Polygon) v).xpoints.length; i++) {
						add.addPoint(((Polygon) v).xpoints[i], ((Polygon) v).xpoints[i]);

						htblColNameValue1.put(k, add);
					}
				}
			});

			Object[] args = htblColNameValue.values().toArray();
			Object[] cols = htblColNameValue.keySet().toArray();

			for (int i = 0; i < Meta.size(); i++) {
				if (Meta.get(i)[0].equals(strTableName)) {
					MetaIndex = i;
					break;

				}

				if (MetaIndex >= 0)
					break;

			}

			if (MetaIndex == -1) {
				throw (new DBAppException("Table is not found , please make sure that the table name is correct"));
			}
			for (int i = MetaIndex, j = 0; j < args.length && i < Meta.size(); i++, j++) {
				if (!args[j].getClass().getCanonicalName().equalsIgnoreCase(Meta.get(i)[2])
						|| !(cols[j].equals(Meta.get(i)[1]))) {
					throw (new DBAppException(
							"Please make sure that the inserted data matches the expected format of the table and try again"));
				}

			}

			Table RequestedTable = (Table) DeSerialize("data" + "/" + strTableName + ".ser");

			int pages = RequestedTable.pages;
			Page p = null;

			System.out.println(
					"\n>>>INSERTING:" + (htblColNameValue1.get(RequestedTable.strClusteringKeyColumn)) + "<<<<<\n");

			if (RequestedTable.pages == 0) {

				System.out.println("first page");
				p = new Page();

				p.arrRecords.add(htblColNameValue1);
				RequestedTable.pages++;
				Object[] minMax = new Object[2];
				minMax[0] = p.arrRecords.get(0).get(RequestedTable.strClusteringKeyColumn);
				minMax[1] = p.arrRecords.get(p.arrRecords.size() - 1).get(RequestedTable.strClusteringKeyColumn);

				RequestedTable.Ranges.add(minMax);
				serialize("data" + "/" + strTableName + ".ser", RequestedTable);
				serialize("data" + "/" + strTableName + RequestedTable.pages + ".ser", p);

				// TODO Indexing0
				for (int i = 0; i < cols.length; i++) {
					File f = new File("data/" + strTableName + "IndexOn" + cols[i] + ".ser");
					if (f.exists()) {
						Object Index = DeSerialize("data/" + strTableName + "IndexOn" + cols[i] + ".ser");
						if (Index instanceof BPlusTree) {
							BPlusTree bpt = ((BPlusTree) Index);
							bpt.insert((Comparable) htblColNameValue1.get(bpt.columnName), 1);
						} else if (Index instanceof RTree) {
							RTree rt = ((RTree) Index);
							rt.insert((Comparable) ((ProcessPolygon) htblColNameValue1.get(rt.columnName)).calcArea(),
									1);
						}

						serialize("data/" + strTableName + "IndexOn" + cols[i] + ".ser", Index);
					}
				}
				// end of Indexing

				return;
			}

			else if (RequestedTable != null && RequestedTable.pages > 0
					&& RequestedTable.pages <= Integer.parseInt(prop.getProperty("N"))) {

				LinkedHashMap<String, Object> ToBeInserted = htblColNameValue1;
				Object ClusterCol = ToBeInserted.get(RequestedTable.strClusteringKeyColumn);

				boolean nowShifting = false;
				Integer PageForShiftedNode = null;

				int i = 1;

				if (RequestedTable.ClusterIndexed == true) {
					Object Index = DeSerialize(
							"data/" + strTableName + "IndexOn" + RequestedTable.strClusteringKeyColumn + ".ser");

					if (Index instanceof BPlusTree)
						i = ((BPlusTree) Index).getNodeFirstPage(
								(Comparable) htblColNameValue1.get(RequestedTable.strClusteringKeyColumn),
								RequestedTable.pages);
					else if (Index instanceof RTree)
						i = ((RTree) Index)
								.getNodeFirstPage(
										(Comparable) ((ProcessPolygon) htblColNameValue1
												.get(RequestedTable.strClusteringKeyColumn)).calcArea(),
										RequestedTable.pages);

					System.out.println(
							"-----Index found on Clustering Column and Used to find the Page to Insert In-----");

				}

				for (; i <= RequestedTable.pages; i++) {

					File nextF = new File("data" + "/" + strTableName + "" + (i + 1) + ".ser");
					Object[] MinMax = RequestedTable.Ranges.get(i - 1);

					if (compareTo(ToBeInserted.get(RequestedTable.strClusteringKeyColumn), MinMax[1]) <= 0
							|| !nextF.exists()) {
						p = (Page) DeSerialize("data" + "/" + strTableName + "" + i + ".ser");

						boolean ReachedTheEndOfPage = false;
						boolean PageFull = false;

						// The page is not full yet so we search for the correct place to insert

						if (p != null) {

							int j = -1;

							/*
							 * for (j = 0; j < p.arrRecords.size() && j <=
							 * Integer.parseInt(prop.getProperty("X")); j++) {
							 * 
							 * if (compareTo(p.arrRecords.get(j).get(RequestedTable.strClusteringKeyColumn),
							 * ((ToBeInserted.get(RequestedTable.strClusteringKeyColumn)))) >= 0) { break; }
							 * }
							 */
							int first = 0;
							int last = p.arrRecords.size() - 1;
							// int mid = (first + last) / 2;

							if (compareTo(ToBeInserted.get(RequestedTable.strClusteringKeyColumn), MinMax[0]) <= 0) {
								j = 0;
							} else {
								while (first <= last) {

									int mid = (first + last) / 2;
									int comp = compareTo(ToBeInserted.get(RequestedTable.strClusteringKeyColumn),
											p.arrRecords.get(mid).get(RequestedTable.strClusteringKeyColumn));

									if (comp >= 0) {
										first = mid + 1;
									} else {
										j = mid;
										last = mid - 1;
									}

									/*
									 * if (comp == 0) { break; }
									 * 
									 * if ((mid - 1) >= 0) { int compPrev = compareTo(
									 * ToBeInserted.get(RequestedTable.strClusteringKeyColumn), p.arrRecords.get(mid
									 * - 1).get(RequestedTable.strClusteringKeyColumn)); if (comp >= 0 && compPrev
									 * <= 0) break; } else { break; }
									 * 
									 * if (comp > 0) { first = mid + 1; } else { last = mid - 1; }
									 * 
									 * 
									 * int mid = (first + last) / 2;
									 */

								}
								if (j == -1) {
									j = p.arrRecords.size();
								}

								// j = mid;

							}

							if (p.arrRecords.size() == X)
								PageFull = true;
							if (j == X)
								ReachedTheEndOfPage = true;

							boolean DeletionException = false;
							if (!PageFull && j == p.arrRecords.size()) {
								// Page with a deleted item ; j will not reach X but we need to check the rest
								// of the pages
								File nextpage = new File("data" + "/" + strTableName + "" + (i + 1) + ".ser");
								if (nextpage.exists()) {
									DeletionException = true;
								}
							}

							if (!PageFull && !ReachedTheEndOfPage && !DeletionException) {
								// in page and page has space
								System.out.println("normal insert");
								p.arrRecords.add(j, ToBeInserted);
								RequestedTable.Ranges.get(i - 1)[0] = p.arrRecords.get(0)
										.get(RequestedTable.strClusteringKeyColumn);
								RequestedTable.Ranges.get(i - 1)[1] = p.arrRecords.get(p.arrRecords.size() - 1)
										.get(RequestedTable.strClusteringKeyColumn);
								serialize("data" + "/" + strTableName + i + ".ser", p);
								serialize("data" + "/" + strTableName + ".ser", RequestedTable);

								// TODO Indexing1
								for (int z = 0; z < cols.length; z++) {
									File f = new File("data/" + strTableName + "IndexOn" + cols[z] + ".ser");
									if (f.exists()) {
										Object Index = DeSerialize(
												"data/" + strTableName + "IndexOn" + cols[z] + ".ser");
										if (Index instanceof BPlusTree) {

											BPlusTree bpt = ((BPlusTree) Index);
											if (!nowShifting)
												bpt.insert((Comparable) ToBeInserted.get(bpt.columnName), i);
											else {
												bpt.UpdateNode((Comparable) ToBeInserted.get(bpt.columnName),
														PageForShiftedNode, i);
											}
										} else if (Index instanceof RTree) {

											RTree rt = ((RTree) Index);
											if (!nowShifting)
												rt.insert(
														(Comparable) ((ProcessPolygon) ToBeInserted.get(rt.columnName))
																.calcArea(),
														i);
											else {
												rt.UpdateNode(
														(Comparable) ((ProcessPolygon) ToBeInserted.get(rt.columnName))
																.calcArea(),
														PageForShiftedNode, i);
											}
										}
										serialize("data/" + strTableName + "IndexOn" + cols[z] + ".ser", Index);
									}
								}
								// end of Indexing

								return;
							}

							else if (!ReachedTheEndOfPage && PageFull && !DeletionException) {
								// in page but page is full so we lazy shift

								System.out.println("inserting and shifting");
								System.out.println("size of page " + i + " so far is : " + p.arrRecords.size());

								LinkedHashMap<String, Object> ToBeShifted = p.arrRecords.elementAt(X - 1);
								ToBeShifted.replace("TouchDate", dtf.format(now));
								p.arrRecords.remove(X - 1);
								p.arrRecords.add(j, ToBeInserted);

								// TODO Indexing2
								for (int z = 0; z < cols.length; z++) {
									File f = new File("data/" + strTableName + "IndexOn" + cols[z] + ".ser");
									if (f.exists()) {
										Object Index = DeSerialize(
												"data/" + strTableName + "IndexOn" + cols[z] + ".ser");

										if (Index instanceof BPlusTree) {
											BPlusTree bpt = ((BPlusTree) Index);

											if (!nowShifting)
												bpt.insert((Comparable) ToBeInserted.get(bpt.columnName), i);
											else {
												bpt.UpdateNode((Comparable) ToBeInserted.get(bpt.columnName),
														PageForShiftedNode, i);
											}
										} else if (Index instanceof RTree) {

											RTree rt = ((RTree) Index);
											if (!nowShifting)
												rt.insert(
														(Comparable) ((ProcessPolygon) ToBeInserted.get(rt.columnName))
																.calcArea(),
														i);
											else {
												rt.UpdateNode(
														(Comparable) ((ProcessPolygon) ToBeInserted.get(rt.columnName))
																.calcArea(),
														PageForShiftedNode, i);
											}

										}

										serialize("data/" + strTableName + "IndexOn" + cols[z] + ".ser", Index);
									}
								}
								// end of Indexing
								RequestedTable.Ranges.get(i - 1)[0] = p.arrRecords.get(0)
										.get(RequestedTable.strClusteringKeyColumn);
								RequestedTable.Ranges.get(i - 1)[1] = p.arrRecords.get(p.arrRecords.size() - 1)
										.get(RequestedTable.strClusteringKeyColumn);
								ToBeInserted = ToBeShifted;
								PageForShiftedNode = i;
								nowShifting = true;

								serialize("data" + "/" + strTableName + i + ".ser", p);
								serialize("data/" + RequestedTable.strTableName + ".ser", RequestedTable);
							}

						}
					}
				}
				// new page required
				System.out.println("new page was required");
				p = new Page();
				Object[] rangetemp = new Object[2];
				rangetemp[0] = ToBeInserted.get(RequestedTable.strClusteringKeyColumn);
				rangetemp[1] = ToBeInserted.get(RequestedTable.strClusteringKeyColumn);
				RequestedTable.Ranges.add(rangetemp);
				p.arrRecords.add(ToBeInserted);

				RequestedTable.pages++;

				// TODO Indexing3
				for (int z = 0; z < cols.length; z++) {
					File f = new File("data/" + strTableName + "IndexOn" + cols[z] + ".ser");
					if (f.exists()) {
						Object Index = DeSerialize("data/" + strTableName + "IndexOn" + cols[z] + ".ser");

						if (Index instanceof BPlusTree) {
							BPlusTree bpt = ((BPlusTree) Index);
							if (!nowShifting)
								bpt.insert((Comparable) ToBeInserted.get(bpt.columnName), RequestedTable.pages);
							else {
								bpt.UpdateNode((Comparable) ToBeInserted.get(bpt.columnName), PageForShiftedNode,
										RequestedTable.pages);
							}
						} else if (Index instanceof RTree) {
							RTree rt = ((RTree) Index);
							if (!nowShifting)
								rt.insert((Comparable) ((ProcessPolygon) ToBeInserted.get(rt.columnName)).calcArea(),
										RequestedTable.pages);
							else {
								rt.UpdateNode(
										(Comparable) ((ProcessPolygon) ToBeInserted.get(rt.columnName)).calcArea(),
										PageForShiftedNode, RequestedTable.pages);
							}
						}
						serialize("data/" + strTableName + "IndexOn" + cols[z] + ".ser", Index);
					}
				}
				// end of Indexing

				serialize("data" + "/" + strTableName + RequestedTable.pages + ".ser", p);
				RequestedTable.Ranges.get(RequestedTable.pages - 1)[0] = p.arrRecords.get(p.arrRecords.size() - 1)
						.get(RequestedTable.strClusteringKeyColumn);

				RequestedTable.Ranges.get(RequestedTable.pages - 1)[1] = p.arrRecords.get(0)
						.get(RequestedTable.strClusteringKeyColumn);
				serialize("data" + "/" + strTableName + ".ser", RequestedTable);

				return;
			} else {
				throw (new DBAppException(
						"The current table has reached the maximum number of pages, consider changing the properties file"));
			}
		} catch (IOException E) {
			E.printStackTrace();
		}
	}

	public static boolean PolygonEquals(ProcessPolygon p1, ProcessPolygon p2) {
		// method to check for equality for polygons
		int[] p1x = p1.xpoints;
		int[] p1y = p1.ypoints;
		int[] p2x = p2.xpoints;
		int[] p2y = p2.ypoints;
		for (int i = 0; i < p1x.length && i < p2x.length; i++) {
			if (p1x[i] != p2x[i] || p1y[i] != p2y[i])
				return false;
		}
		return true;
	}

	public int FindBinarySearch(Vector<LinkedHashMap<String, Object>> Page, String strClusteringColumn, int first,
			int last, Object key) {
		int mid = (first + last) / 2;
		while (first <= last) {
			if (compareTo(key, Page.get(mid).get(strClusteringColumn)) > 0) {
				first = mid + 1;
			} else if (compareTo(Page.get(mid).get(strClusteringColumn), key) == 0) {
				return mid;

			} else { // System.out.println("got here");
				last = mid - 1;
			}
			mid = (first + last) / 2;
		}

		return -1;
	}

	public int InsertBinarySearch(Vector<LinkedHashMap<String, Object>> Page, String strClusteringColumn, int first,
			int last, Object key) {
		int mid = (first + last) / 2;
		while (first <= last) {
			if (compareTo(key, Page.get(mid).get(strClusteringColumn)) < 0) {
				System.out.println(Page.get(mid).get(strClusteringColumn));
				first = mid + 1;
			} else if (compareTo(key, Page.get(mid).get(strClusteringColumn)) >= 0) {
				return mid;

			} else { // System.out.println("got here");
				last = mid - 1;
			}
			mid = (first + last) / 2;
		}
		if (first > last) {
			System.out.println("Element is not found!");
		}
		return -1;
	}

	public static boolean Equals(LinkedHashMap<String, Object> R1, LinkedHashMap<String, Object> R2) {
		// R1 is the input hashtable/linkedhashmap in the delete method , it could have
		// empty columns
		// R2 is the retrieved linkedhashmap from the table
		R1.put("EQ", true);
		// System.out.println("R1 is empty or not ?: " + R1.isEmpty());
		R1.forEach((k, v) -> {
			if (v instanceof Polygon) {
				if (!(R2.containsKey(k) && PolygonEquals((ProcessPolygon) R2.get(k), (ProcessPolygon) v))
						&& !k.equals("EQ")) {
					R1.replace("EQ", false);

				}

			} else {

				if (!(R2.containsKey(k) && R2.get(k).equals(v)) && !k.equals("EQ"))
					R1.replace("EQ", false);

			}

		});
		return ((boolean) R1.get("EQ"));
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		// we still need to make sure that all the rows in all pages are sorted after
		// the deletion
		int MetaIndex = -1;

		if (htblColNameValue.isEmpty())
			throw (new DBAppException("Cannot delete an empty row!"));
		Object[] args = htblColNameValue.values().toArray();
		Object[] cols = htblColNameValue.keySet().toArray();

		for (int i = 0; i < Meta.size(); i++) {
			if (Meta.get(i)[0].equals(strTableName)) {
				MetaIndex = i;
				break;

			}

			if (MetaIndex >= 0)
				break;

		}

		if (MetaIndex == -1) {
			throw (new DBAppException("Table is not found , please make sure that the table name is correct"));
		}
		for (int i = MetaIndex, j = 0; j < args.length && i < Meta.size(); i++, j++) {
			if (!args[j].getClass().getCanonicalName().equalsIgnoreCase(Meta.get(i)[2])
					&& (cols[j].equals(Meta.get(i)[1]))) {
				System.out.println(cols[j] + "     and  " + Meta.get(i)[1]);
				throw (new DBAppException(
						"Please make sure that the inserted data matches the expected format of the table and try again"));
			}

		}

		boolean optimized = false;
		LinkedHashMap<String, Object> ToBeDeleted = new LinkedHashMap<String, Object>();
		Table RequestedTable = (Table) DeSerialize("data" + "/" + strTableName + ".ser");
		htblColNameValue.forEach((k, v) -> {
			// converting to linkedhashmap to make it compatible with our methods
			if (!(v instanceof Polygon))
				ToBeDeleted.put(k, v);
			else {
				ProcessPolygon add = new ProcessPolygon();
				for (int i = 0; i < ((Polygon) v).xpoints.length; i++) {
					add.addPoint(((Polygon) v).xpoints[i], ((Polygon) v).xpoints[i]);

					ToBeDeleted.put(k, add);
				}
			}
		});

		Object bpt = null;
		// int IndexOfbpt;
		Object[] keys = ToBeDeleted.keySet().toArray();
		boolean Indexed = false;
		boolean ClusterIndexed = false;
		int pages = RequestedTable.pages;
		if (htblColNameValue.get(RequestedTable.strClusteringKeyColumn) != null)
			optimized = true;
		String[] colNames = (String[]) RequestedTable.htblColNameType.keySet().toArray();

		if (RequestedTable.ClusterIndexed && RequestedTable.indicies > 1)
			Indexed = true;

		if (optimized && !Indexed) {
			// optimized mean that since there is the clustering column in the input
			// hashtable then we can slightly optimize the search using the p.min &p.max
			for (int i = 1; i <= pages; i++) {
				Object[] MinMax = RequestedTable.Ranges.get(i - 1);
				if (compareTo(MinMax[1], ToBeDeleted.get(RequestedTable.strClusteringKeyColumn)) >= 0
						&& compareTo(MinMax[0], ToBeDeleted.get(RequestedTable.strClusteringKeyColumn)) <= 0) {
					Page p = (Page) DeSerialize("data" + "/" + strTableName + i + ".ser");
					for (int j = 0; j < p.arrRecords.size(); j++) {
						if (Equals(ToBeDeleted, p.arrRecords.get(j))) {
							p.arrRecords.remove(j);
							if (!p.arrRecords.isEmpty()) {

								RequestedTable.Ranges.get(i - 1)[0] = p.arrRecords.get(0)
										.get(RequestedTable.strClusteringKeyColumn);
								RequestedTable.Ranges.get(i - 1)[1] = p.arrRecords.get(p.arrRecords.size() - 1)
										.get(RequestedTable.strClusteringKeyColumn);

							}

							j--;
							// j-- because the vector is automatically shifted after deletion so we don't
							// skip some elements

						}

					}

					try {
						// we have to organize the exception handling
						serialize("data" + "/" + strTableName + i + ".ser", p);
						serialize("data/" + strTableName + ".ser", RequestedTable);
					} catch (Exception e) {

					}
					if (p != null && p.arrRecords.isEmpty()) {
						shiftpages(i, strTableName);
						i--;
						// empty page => then we shift the pages after the empty page and remove the
						// empty page and we decrement the no. of pages of the table
					}
				}

			}

		} else if (!Indexed && !optimized) {
			for (int i = 1; i <= pages; i++) {
				Page p = (Page) DeSerialize("data" + "/" + strTableName + i + ".ser");

				if (p != null)
					for (int j = 0; j < p.arrRecords.size(); j++) {
						System.out.println(ToBeDeleted + " , " + p.arrRecords.get(j));
						if (Equals(ToBeDeleted, p.arrRecords.get(j))) {
							System.out.println(ToBeDeleted + " , " + p.arrRecords.get(j));
							p.arrRecords.remove(j);
							if (!p.arrRecords.isEmpty())

								j--;
						}

					}

				if (p != null && p.arrRecords.isEmpty()) {
					System.out.println("Page number " + i + " should be shifted");

					shiftpages(i, strTableName);
					i--;
				}

				else

					serialize("data" + "/" + strTableName + i + ".ser", p);

			}

		} else if (RequestedTable.ClusterIndexed) {

			boolean emptyPage = false;
			Object Index = DeSerialize(
					"data/" + strTableName + "IndexOn" + RequestedTable.strClusteringKeyColumn + ".ser");

			Vector Occurences = null;
			if (Index instanceof BPlusTree) {
				Occurences = ((BPlusTree) Index).search((Comparable) ToBeDeleted.get(((BPlusTree) Index).columnName));
			} else if (Index instanceof RTree) {
				// Occurences=((BPlusTree) Index).search((Comparable)
				// ToBeDeleted.get(((BPlusTree) Index).columnName));
			}

			for (int i = 0; i < Occurences.size(); i++) {

				Page page = (Page) DeSerialize("data/" + strTableName + "" + ((int[]) Occurences.get(i))[0] + ".ser");

				int ind = FindBinarySearch(page.arrRecords, RequestedTable.strClusteringKeyColumn, 0,
						page.arrRecords.size() - 1, ToBeDeleted.get(RequestedTable.strClusteringKeyColumn));
				int start = this.goBack(page.arrRecords, ind, ToBeDeleted, RequestedTable.strClusteringKeyColumn);

				System.out.println("Start of binary search index is " + start);
				for (int j = start; j >= 0 && j < page.arrRecords.size(); j++) {
					System.out.println(ToBeDeleted + "   and   " + page.arrRecords.get(j));
					if (Equals(ToBeDeleted, (LinkedHashMap) page.arrRecords.get(j))) {
						System.out.println("got here in page " + ((int[]) Occurences.get(i))[0]);

						LinkedHashMap<String, Object> x = page.arrRecords.remove(j);

						for (int col = 0; col < colNames.length; col++) {

							Object Index1 = DeSerialize("data/" + strTableName + "IndexOn" + colNames[col] + ".ser");
							if (Index1 instanceof BPlusTree)
								((BPlusTree) Index1).delete((Comparable) x.get(((BPlusTree) Index1).columnName),
										((int[]) Occurences.get(i))[0]);
							else if (Index1 instanceof RTree) {
								// ((RTree) Index1).delete((Comparable) x.get(((RTree)
								// Index1).columnName),((int[]) Occurences.get(i))[0]);
							}
							serialize("data/" + strTableName + "IndexOn" + colNames[col] + ".ser", Index1);

						}

						j--;
					}

				}

				if (!page.arrRecords.isEmpty()) {
					// updateBelowWithinPage(strTableName, page.arrRecords, ((int[])
					// Occurences.get(i))[1],
					// ((int[]) Occurences.get(i))[0]);
					serialize("data/" + strTableName + "" + ((int[]) Occurences.get(i))[0] + ".ser", page);

				} else
					shiftpages(((int[]) Occurences.get(i))[0], strTableName);

				// serialize("data/" + strTableName + "Index" + IndexOfbpt + ".ser", bpt);

			}

		}

		else if (!ClusterIndexed && Indexed) {
			System.out.println("I go thereee");
			for (int i = 1; i <= RequestedTable.pages; i++) {
				Page p = (Page) DeSerialize("data/" + strTableName + i + ".ser");
				Vector page = p.arrRecords;
				for (int j = 0; j < page.size(); j++) {
					if (Equals(ToBeDeleted, (LinkedHashMap) page.get(j))) {
						LinkedHashMap<String, Object> x = (LinkedHashMap<String, Object>) page.remove(j);
						j--;
						System.out.println(x);
						for (int col = 0; col < colNames.length; col++) {

							Object Index = DeSerialize("data/" + strTableName + "IndexOn" + colNames[col] + ".ser");
							if (Index instanceof BPlusTree)
								((BPlusTree) Index).delete((Comparable) x.get(((BPlusTree) Index).columnName), i);
							else if (Index instanceof RTree)
								((RTree) Index).delete((Comparable) x.get(((RTree) Index).columnName), i);
							serialize("data/" + strTableName + "IndexOn" + colNames[col] + ".ser", bpt);

						}

					}
					serialize("data/" + strTableName + i + ".ser", p);
				}

			}

		}

	}

	public int goBack(Vector<LinkedHashMap<String, Object>> page, int lastOccurence,
			LinkedHashMap<String, Object> ToBeDeleted, String strClusteringKey) {
		int i;
		for (i = lastOccurence; i < page.size() && i > 0; i--) {
			if (!(page.get(i).get(strClusteringKey).equals(ToBeDeleted.get(strClusteringKey))))
				break;
		}
		return i;
	}

	private void shiftpages(int i, String strTableName) {
		Table TargetedTable = (Table) DeSerialize("data" + "/" + strTableName + ".ser");
		File EmptyPage = new File("data" + "/" + strTableName + i + ".ser");
		if (EmptyPage.exists())
			EmptyPage.delete();
		for (int j = i + 1; j <= TargetedTable.pages; j++) {
			File OldPage = new File("data" + "/" + strTableName + j + ".ser");
			int shift = j - 1;
			Page page = (Page) DeSerialize("data/" + strTableName + j + ".ser");

			File shifted = new File("data" + "/" + strTableName + shift + ".ser");

			for (int k = 1; k <= TargetedTable.indicies; k++)

				OldPage.renameTo(shifted);
		}
		TargetedTable.pages--;
		serialize("data" + "/" + strTableName + ".ser", TargetedTable);

	}

	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		try {

			File Table = new File("data" + "/" + strTableName + ".ser");
			Table RequestedTable = null;
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			dateFormat.setLenient(false);
			Object Cluster = null;
			if (Table.exists()) {

				RequestedTable = (Table) DeSerialize("data" + "/" + strTableName + ".ser");

				boolean ClusterIndexed = false;

				if (htblColNameValue.get(RequestedTable.strClusteringKeyColumn) != null)
					throw (new DBAppException("Cannot edit the clustering key"));
				String x = RequestedTable.htblColNameType.get(RequestedTable.strClusteringKeyColumn).toLowerCase();
				if (x.equals(Integer.class.getCanonicalName().toLowerCase())) {

					Cluster = Integer.parseInt(strClusteringKey);

				} else if (x.equals(Double.class.getCanonicalName().toLowerCase())) {

					Cluster = Double.parseDouble(strClusteringKey);
					System.out.println(Cluster);

				} else if (x.equals(String.class.getCanonicalName().toLowerCase())) {

					Cluster = strClusteringKey;
					System.out.println(Cluster);

				} else if (x.equals(Polygon.class.getCanonicalName().toLowerCase())) {
					StringTokenizer s = new StringTokenizer(strClusteringKey, ",()");
					ArrayList<Integer> xpointsarrl = new ArrayList<>();
					ArrayList<Integer> ypointsarrl = new ArrayList<>();
					int j = 0;
					Cluster = new ProcessPolygon();
					while (s.hasMoreTokens()) {
						if (j % 2 == 0)
							xpointsarrl.add(Integer.parseInt(s.nextToken()));
						else
							ypointsarrl.add((Integer.parseInt(s.nextToken())));
						j++;
					}

					for (j = 0; j < xpointsarrl.size(); j++)
						((ProcessPolygon) Cluster).addPoint(xpointsarrl.get(j), ypointsarrl.get(j));
				} else if (x.equals(Date.class.getCanonicalName().toLowerCase())) {
					Cluster = dateFormat.parse(strClusteringKey);

					System.out.println(dateFormat.parse(strClusteringKey));
				}

				if (!RequestedTable.ClusterIndexed) {

					for (int i = 1; i <= RequestedTable.pages; i++) {
						Object[] MinMax = RequestedTable.Ranges.get(i - 1);
						if (compareTo(Cluster, MinMax[1]) <= 0 && compareTo(Cluster, MinMax[0]) >= 0) {
							Page p = (Page) DeSerialize("data" + "/" + strTableName + i + ".ser");

							for (int j = 0; j < p.arrRecords.size(); j++) {
								System.out.println(p.arrRecords.get(j).get(RequestedTable.strClusteringKeyColumn)
										+ " , " + Cluster);
								if (!(MinMax[1] instanceof ProcessPolygon)
										&& compareTo(p.arrRecords.get(j).get(RequestedTable.strClusteringKeyColumn),
												Cluster) == 0) {
									// check that polygons are equal???

									LinkedHashMap<String, Object> temp = p.arrRecords.get(j);
									temp.forEach((k, v) -> {
										htblColNameValue.forEach((k1, v1) -> {
											if (k.equals(k1) && v1 != null) {
												if (!(v1 instanceof Polygon))
													temp.replace(k, v1);
												else {
													ProcessPolygon add = new ProcessPolygon();
													for (int z = 0; z < ((Polygon) v).xpoints.length; z++) {
														add.addPoint(((Polygon) v).xpoints[z],
																((Polygon) v).xpoints[z]);

														temp.put(k, add);
													}
												}

												LocalDateTime now = LocalDateTime.now();

												temp.replace("TouchDate", dtf.format(now));
											} else if (k.equals(k1) && v1 != null
													&& !(v1.getClass().getCanonicalName().toLowerCase()
															.equals(v.getClass().getCanonicalName().toLowerCase()))) {
												temp.put("Type", true);
											}

										});
									});

								} else if (MinMax[1] instanceof ProcessPolygon
										&& ((ProcessPolygon) Cluster).equals((ProcessPolygon) p.arrRecords.get(j)
												.get(RequestedTable.strClusteringKeyColumn))) {
									LinkedHashMap<String, Object> temp = p.arrRecords.get(j);
									temp.forEach((k, v) -> {
										htblColNameValue.forEach((k1, v1) -> {
											if (k.equals(k1) && v1 != null) {
												LocalDateTime now = LocalDateTime.now();
												if (!(v1 instanceof Polygon))
													temp.replace(k, v1);
												else {
													ProcessPolygon add = new ProcessPolygon();
													for (int z = 0; z < ((Polygon) v).xpoints.length; z++) {
														add.addPoint(((Polygon) v).xpoints[z],
																((Polygon) v).xpoints[z]);

														temp.put(k, add);
													}
												}
												temp.replace("Touchdate", dtf.format(now));

											} else if (k.equals(k1) && v1 != null
													&& !(v1.getClass().getCanonicalName().toLowerCase()
															.equals(v.getClass().getCanonicalName().toLowerCase()))) {
												temp.put("Type", true);
											}

										});
									});
									if (temp.get("Type") != null)
										throw (new DBAppException(
												"Please make sure that the data inserted into the update method matches the actual data"));
								}

							}

						}

					}
				} else if (RequestedTable.ClusterIndexed) {
					Object Index = DeSerialize(
							"data/" + strTableName + "IndexOn" + RequestedTable.strClusteringKeyColumn + ".ser");
					if (Index instanceof RTree)
						Index = (RTree) Index;
					else if (Index instanceof BPlusTree) {

						Index = (BPlusTree) Index;
						Vector Occurences = ((BPlusTree) Index).search((Comparable) Cluster);
						for (int i = 0; i < Occurences.size(); i++) {
							Page p = (Page) DeSerialize(
									"data/" + strTableName + ((int[]) Occurences.get(i))[0] + ".ser");
							int last = FindBinarySearch(p.arrRecords, ((BPlusTree) Index).columnName, 0,
									p.arrRecords.size() - 1, Cluster);
							LinkedHashMap<String, Object> temp = new LinkedHashMap<String, Object>();
							temp.put(((BPlusTree) Index).columnName, Cluster);
							int startIndex = goBack(p.arrRecords, last, temp, ((BPlusTree) Index).columnName);
							for (int j = startIndex; j < p.arrRecords.size(); j++) {
								if (Equals(temp, p.arrRecords.get(j))) {
									LinkedHashMap<String, Object> temp1 = p.arrRecords.get(j);
									temp1.forEach((k, v) -> {
										htblColNameValue.forEach((k1, v1) -> {
											if (k.equals(k1) && v1 != null) {
												if (!(v1 instanceof Polygon))
													temp1.replace(k, v1);

												else {
													ProcessPolygon add = new ProcessPolygon();
													for (int z = 0; z < ((Polygon) v).xpoints.length; z++) {
														add.addPoint(((Polygon) v).xpoints[z],
																((Polygon) v).xpoints[z]);

														temp1.put(k, add);
													}
												}
												LocalDateTime now = LocalDateTime.now();

												temp1.replace("TouchDate", dtf.format(now));
											} else if (k.equals(k1) && v1 != null
													&& !(v1.getClass().getCanonicalName().toLowerCase()
															.equals(v.getClass().getCanonicalName().toLowerCase()))) {
												temp1.put("Type", true);
											}

										});
									});

								}
							}
							serialize("data" + "/" + strTableName + i + ".ser", p);
						}

					}

				}
			}
		}

		catch (ParseException E1) {

		}

	}

	public int compareTo(Object O1, Object O2) {
		if (O1 instanceof Integer && O2 instanceof Integer) {

			return (((Integer) O1).compareTo((int) O2));
		} else if (O1 instanceof Double && O2 instanceof Double) {

			return (((Double) O1).compareTo((Double) O2));
		} else if (O1 instanceof String && O2 instanceof String) {
			return (((String) O1).compareTo((String) O2));
		} else if (O1 instanceof Polygon && O2 instanceof Polygon) {
			Polygon x = (Polygon) O1;
			ProcessPolygon t = new ProcessPolygon();
			// ProcessPolygon => our version of polygon to implement comparable interface so
			// we can sort polygons
			for (int i = 0; i < x.npoints; i++) {
				t.addPoint(x.xpoints[i], x.ypoints[i]);
			}
			return t.compareTo(O2);

		} else if (O1 instanceof Date && O2 instanceof Date)
			return ((Date) O1).compareTo((Date) O2);

		else if (O1 instanceof Boolean && O2 instanceof Boolean)
			return (((Boolean) O1).compareTo((Boolean) O2));
		else
			return 2;

	}

	public static Object DeSerialize(String File) {

		try {
			FileInputStream fileInputStream = new FileInputStream(File);
			BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
			ObjectInputStream objectInputStream = new ObjectInputStream(bufferedInputStream);

			Object object = objectInputStream.readObject();
			objectInputStream.close();
			return object;

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	public static void serialize(String File, Object Data) {
		try {
			FileOutputStream file = new FileOutputStream(File);
			ObjectOutputStream out = new ObjectOutputStream(file);

			// Method for serialization of object
			out.writeObject(Data);

			out.close();
			file.close();

			System.out.println("Object has been serialized in: " + File);

		} catch (IOException E) {

		}
	}

	public static int randBetween(int start, int end) {
		return start + (int) Math.round(Math.random() * (end - start));
	}

	public void createBTreeIndex(String strTableName, String strColName) throws DBAppException, IOException {
		Properties P = new Properties();
		FileReader reader = new FileReader("config/DBApp.properties");

		P.load(reader);
		int Order = Integer.parseInt(P.getProperty("X")) + 1;
		int MetaIndex = -1;
		for (int i = 0; i < Meta.size(); i++) {
			if (Meta.get(i)[0].equals(strTableName)) {
				MetaIndex = i;
				break;
			}
			/*
			 * if (MetaIndex >= 0) break;
			 */
		}

		if (MetaIndex == -1) {
			throw (new DBAppException("Table is not found , please make sure that the table name is correct"));
		}

		boolean columnFound = false;
		String columnClass = "";
		for (int i = MetaIndex; Meta.get(i)[0].equals(strTableName) && i < Meta.size(); i++) {
			if (Meta.get(i)[1].equals(strColName)) {
				columnFound = true;
				columnClass = Meta.get(i)[2];
				break;
			}
		}

		if (!columnFound) {
			throw (new DBAppException("Column is not found , please make sure that the Column name is correct"));
		} else {

			Table RequestedTable = (Table) DeSerialize("data" + "/" + strTableName + ".ser");
			RequestedTable.indicies++;
			BPlusTree bpt = null;

			switch (columnClass) {
			case "java.lang.Integer":
				bpt = new BPlusTree<Integer, int[]>(Order, strTableName, strColName, RequestedTable.indicies);
				break;
			case "java.lang.String":
				bpt = new BPlusTree<String, int[]>(Order, strTableName, strColName, RequestedTable.indicies);
				break;
			case "java.lang.Double":
				bpt = new BPlusTree<Double, int[]>(Order, strTableName, strColName, RequestedTable.indicies);
				break;
			case "java.lang.Boolean":
				bpt = new BPlusTree<Boolean, int[]>(Order, strTableName, strColName, RequestedTable.indicies);
				break;
			case "java.util.Date":
				bpt = new BPlusTree<Date, int[]>(Order, strTableName, strColName, RequestedTable.indicies);
				break;
			case "java.awt.Polygon":
				RequestedTable.indicies--;
				throw new DBAppException("Cannot create a B-Plus Tree on a polygon!");
			default:
				RequestedTable.indicies--;
				throw (new DBAppException("class type is not found"));
			}

			if (RequestedTable.strClusteringKeyColumn.equals(strColName))
				RequestedTable.ClusterIndexed = true;
			Page p;

			for (int i = 1; i <= RequestedTable.pages; i++) {
				p = (Page) DeSerialize("data" + "/" + strTableName + "" + i + ".ser");
				for (int j = 0; j < p.arrRecords.size(); j++) {
					bpt.insert((Comparable) p.arrRecords.get(j).get(strColName), i);
					// System.out.println(bpt);
					System.out.println("\n>>>INSERTED: " + (Comparable) p.arrRecords.get(j).get(strColName) + "\n");
				}
			}
			serialize("data/" + strTableName + "IndexOn" + strColName + ".ser", bpt);
			serialize("data" + "/" + strTableName + ".ser", RequestedTable);
		}
	}

	public void createRTreeIndex(String strTableName, String strColName) throws DBAppException, IOException {
		Properties P = new Properties();
		FileReader reader = new FileReader("config/DBApp.properties");

		P.load(reader);
		int Order = Integer.parseInt(P.getProperty("X")) + 1;
		int MetaIndex = -1;
		for (int i = 0; i < Meta.size(); i++) {
			if (Meta.get(i)[0].equals(strTableName)) {
				MetaIndex = i;
				break;
			}
		}

		if (MetaIndex == -1) {
			throw (new DBAppException("Table is not found , please make sure that the table name is correct"));
		}

		boolean columnFound = false;
		String columnClass = "";
		for (int i = MetaIndex; Meta.get(i)[0].equals(strTableName) && i < Meta.size(); i++) {
			if (Meta.get(i)[1].equals(strColName)) {
				columnFound = true;
				columnClass = Meta.get(i)[2];
				break;
			}
		}

		if (!columnFound) {
			throw (new DBAppException("Column is not found , please make sure that the Column name is correct"));
		} else {

			if (!columnClass.equals("java.awt.Polygon"))
				throw (new DBAppException("Must create a RTree on a Polygon type Column"));

			Table RequestedTable = (Table) DeSerialize("data" + "/" + strTableName + ".ser");
			if (RequestedTable.strClusteringKeyColumn.equals(strColName))
				RequestedTable.ClusterIndexed = true;
			RequestedTable.indicies++;
			Page p;
			RTree bpt = new RTree<Double, int[]>(4, strTableName, strColName, RequestedTable.indicies);

			for (int i = 1; i <= RequestedTable.pages; i++) {
				p = (Page) DeSerialize("data" + "/" + strTableName + "" + i + ".ser");
				for (int j = 0; j < p.arrRecords.size(); j++) {
					ProcessPolygon poly = (ProcessPolygon) p.arrRecords.get(j).get(strColName);
					Double Area = getArea(poly);
					bpt.insert(Area, i);
					System.out.println(
							"\n>>>INSERTED IN INDEX: " + (Comparable) p.arrRecords.get(j).get(strColName) + "\n");
				}
			}
			serialize("data/" + strTableName + "IndexOn" + strColName + ".ser", bpt);
			serialize("data" + "/" + strTableName + ".ser", RequestedTable);
		}

	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		Vector x = new Vector();
		Vector<Vector<LinkedHashMap<String, Object>>> Tuples = new Vector<Vector<LinkedHashMap<String, Object>>>();
		for (int i = 0; i < arrSQLTerms.length; i++) {
			File Ind = new File(
					"data/" + arrSQLTerms[i]._strTableName + "IndexOn" + arrSQLTerms[i]._strColumnName + ".ser");
			if (Ind.exists()) {
				Tuples.add(i, FilterIndex(arrSQLTerms[i]));
			} else {
				Tuples.add(i, FilterNoIndex(arrSQLTerms[i]));
			}
		}
		for (int i = 0; i < strarrOperators.length; i++) {
			switch (strarrOperators[i]) {
			case "AND":
				Vector<LinkedHashMap<String, Object>> andRes = AND(Tuples.remove(0), Tuples.remove(0));
				Tuples.add(0, andRes);
				break;
			case "OR":
				Vector<LinkedHashMap<String, Object>> orRes = OR(Tuples.remove(0), Tuples.remove(0));
				Tuples.add(0, orRes);
				break;
			case "XOR":
				Vector<LinkedHashMap<String, Object>> xorRes = XOR(Tuples.remove(0), Tuples.remove(0));
				Tuples.add(0, xorRes);
				break;
			}
		}

		return Tuples.get(0).iterator();

	}

	public static Vector<LinkedHashMap<String, Object>> AddTuples(Vector<LinkedHashMap<String, Object>> result,
			Vector<LinkedHashMap<String, Object>> intermediate) {

		intermediate.forEach((v) -> {
			if (!result.contains(v))
				result.add(v);
		});

		return result;
	}

	public Vector<LinkedHashMap<String, Object>> FilterLinearAND(ArrayList<SQLTerm> terms) {
		// Cluster index is the index of the SQLTerm that deals with the clustering
		// column , if exists
		int ClusterIndex = -1;
		Table RequestedTable = (Table) DeSerialize("data/" + terms.get(0)._strTableName + ".ser");
		Vector<LinkedHashMap<String, Object>> result = new Vector<LinkedHashMap<String, Object>>();
		for (int i = 0; i < terms.size(); i++)
			if (terms.get(i)._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
				ClusterIndex = i;
				break;
			}

		if (ClusterIndex >= 0) {

			// There is a term that specifies a condition on the clustering column so we
			// will use binary search if it's useful depending on the operator
			SQLTerm ClusterTerm = terms.get(ClusterIndex);
			String ClusterOperator = terms.get(ClusterIndex)._strOperator;
			LinkedHashMap<String, Object> temp = new LinkedHashMap<String, Object>();
			temp.put(ClusterTerm._strColumnName, ClusterTerm._objValue);
			if (ClusterOperator.equals(">") || ClusterOperator.equals(">=") || ClusterOperator.equals("=")) {

				boolean Cont = false;
				// we will use binary search
				for (int i = 1; i <= RequestedTable.pages; i++) {
					// Table.Ranges.get(page number -1)= (min,max) of page
					Object[] MinMax = RequestedTable.Ranges.get(i - 1);

					if (compareTo(ClusterTerm._objValue, MinMax[0]) >= 0
							&& compareTo(ClusterTerm._objValue, MinMax[1]) <= 0) {
						Page p = (Page) DeSerialize("data/" + ClusterTerm._strTableName + i + ".ser");

						int ind = FindBinarySearch(p.arrRecords, RequestedTable.strClusteringKeyColumn, 0,
								p.arrRecords.size() - 1, ClusterTerm._objValue);

						if (ClusterOperator.equals("=") || ClusterOperator.equals(">=") && !Cont) {

							int start = goBack(p.arrRecords, ind, p.arrRecords.get(ind), ClusterTerm._strColumnName);
							for (int j = start; j < p.arrRecords.size(); j++) {

								if (Satisfies(p.arrRecords.get(j).get(ClusterTerm._strColumnName),
										temp.get(ClusterTerm._strColumnName), ClusterTerm._strOperator)) {

									if (SatisfiesAll(p.arrRecords.get(j), terms))
										result.add(p.arrRecords.get(j));
									if (ClusterOperator.equals(">="))
										Cont = true;

									// Cont=true means that we will continue beyond the equal elements (>)

								}

							}

						}

					}
					if (Cont) {
						Page p = (Page) DeSerialize("data/" + ClusterTerm._strTableName + i + ".ser");

						for (int j = 0; j < p.arrRecords.size(); j++) {
							if (SatisfiesAll(p.arrRecords.get(j), terms))
								result.add(p.arrRecords.get(j));
						}
					}

				}

			}

			else {

			}

		} else {
			// normal linear scan since there is no constraint on the clustering column

		}

		return result;
	}

	public boolean Satisfies(Object x1, Object x2, String strOperator) {
		switch (strOperator) {
		case "<":
			return compareTo(x1, x2) < 0;

		case "<=":
			return compareTo(x1, x2) <= 0;

		case ">":
			return compareTo(x1, x2) > 0;

		case ">=":
			return compareTo(x1, x2) >= 0;

		case "=":
			return Equals(x1, x2);

		case "!=":
			return Equals(x1, x2);

		}
		return false;
	}

	private boolean Equals(Object x1, Object x2) {
		// TODO Auto-generated method stub
		return x1.equals(x2);
	}

	public boolean SatisfiesAll(LinkedHashMap<String, Object> x1, ArrayList<SQLTerm> terms) {
		boolean ret = true;

		for (int i = 0; i < terms.size(); i++) {
			if (!Satisfies(x1.get(terms.get(i)._strColumnName), terms.get(i)._objValue, terms.get(i)._strOperator))
				return false;
		}
		return true;
	}

	public Vector<LinkedHashMap<String, Object>> FilterNoIndex(SQLTerm sqlTerm) {
		Table RequestedTable = (Table) DeSerialize("data/" + sqlTerm._strTableName + ".ser");

		Vector<LinkedHashMap<String, Object>> result = new Vector<LinkedHashMap<String, Object>>();

		if (RequestedTable.strClusteringKeyColumn.equals(sqlTerm._strColumnName)) {
			// cluster

			LinkedHashMap<String, Object> temp = new LinkedHashMap<String, Object>();

			for (int i = 1; i <= RequestedTable.pages; i++) {

				// Table.ranges.get(Page no. -1) =(min,max);
				Object[] MinMax = RequestedTable.Ranges.get(i - 1);

				if (sqlTerm._strOperator.equals("=") || sqlTerm._strOperator.equals(">=")
						|| sqlTerm._strOperator.equals(">")) {

					// if the operator is = or > or >= then we will do binary search
					if (compareTo(sqlTerm._objValue, MinMax[1]) <= 0 && compareTo(sqlTerm._objValue, MinMax[0]) >= 0) {
						Page p = (Page) DeSerialize("data/" + sqlTerm._strTableName + i + ".ser");

						Vector<LinkedHashMap<String, Object>> page = p.arrRecords;
						int lastOccurence = FindBinarySearch(page, sqlTerm._strColumnName, 0, page.size() - 1,
								sqlTerm._objValue);

						temp.put(sqlTerm._strColumnName, sqlTerm._objValue);
						int start = goBack(page, lastOccurence, temp, sqlTerm._strColumnName);

						if (sqlTerm._strOperator.equals("="))

							for (int j = start; j < page.size(); j++) {

								if (Equals(temp, page.get(j))) {
									result.add(page.get(j));

								}

							}

						else if (sqlTerm._strOperator.equals(">=")) {
							// if operator is >= then we need the first occurence of the key , hence we use
							// goback
							int j = start;

							result.addAll(page.subList(j, page.size()));
							if (j >= page.size()) {
								return IncludeRestofTable(i + 1, sqlTerm._strTableName, result);

							}

						}

						else if (sqlTerm._strOperator.equals(">")) {

							// if operator is > we will use the last occurence without goback
							if (lastOccurence == page.size() - 1) {

								// if the lastOccurence of a key within a page is the last element , then we
								// need to check the next page before we include the rest of the table
								return checkNextPage(i + 1, sqlTerm._strTableName, sqlTerm._strColumnName,
										sqlTerm._objValue, result);

							} else {
								System.out.println("I got hereee	 " + lastOccurence);

								// if the last occurence is not the last element of the page then we will insert
								// the res
								result.addAll(page.subList(lastOccurence + 1, page.size()));

								return IncludeRestofTable(i + 1, sqlTerm._strTableName, result);

							}

						}

					}
				} else if (sqlTerm._strOperator.equals("<") || sqlTerm._strOperator.equals("<=")) {
					Page p = (Page) DeSerialize("data/" + sqlTerm._strTableName + i + ".ser");

					for (int j = 0; j < p.arrRecords.size(); j++) {
						if (sqlTerm._strOperator.equals("<")
								&& compareTo(sqlTerm._objValue, p.arrRecords.get(j).get(sqlTerm._strColumnName)) < 0)
							result.add(p.arrRecords.get(j));
						else if (sqlTerm._strOperator.equals("<=")
								&& compareTo(sqlTerm._objValue, p.arrRecords.get(j).get(sqlTerm._strColumnName)) <= 0)
							result.add(p.arrRecords.get(j));
						else
							return result;
					}

				}

				else if (sqlTerm._strOperator.equals("!=")) {
					Page p = (Page) DeSerialize("data/" + sqlTerm._strTableName + i + ".ser");
					int j = 0;
					if (compareTo(sqlTerm._objValue, MinMax[1]) <= 0 && compareTo(sqlTerm._objValue, MinMax[0]) >= 0) {
						j = FindBinarySearch(p.arrRecords, sqlTerm._strColumnName, 0, p.arrRecords.size() - 1,
								sqlTerm._objValue);
						int start = goBack(p.arrRecords, j, temp, sqlTerm._strColumnName);
						for (j = 0; j < start; j++)
							if (!Equals(temp, p.arrRecords.get(j))) {
								result.add(p.arrRecords.get(j));
							}

						result.addAll(p.arrRecords.subList(j + 1, p.arrRecords.size() - 1));

					} else
						result.addAll(p.arrRecords);

				}

			}

		} else {
			for (int i = 1; i <= RequestedTable.pages; i++) {
				Page p = (Page) DeSerialize("data/" + sqlTerm._strTableName + i + ".ser");
				for (int j = 0; j < p.arrRecords.size(); j++) {
					if (sqlTerm._strOperator.equals(">")
							&& compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) > 0) {
						result.add(p.arrRecords.get(j));

					} else if (sqlTerm._strOperator.equals(">=")
							&& compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) >= 0) {
						result.add(p.arrRecords.get(j));

					} else if (sqlTerm._strOperator.equals("<")
							&& compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) < 0) {
						result.add(p.arrRecords.get(j));

					} else if (sqlTerm._strOperator.equals("<=")
							&& compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) <= 0) {
						result.add(p.arrRecords.get(j));

					} else if (sqlTerm._strOperator.equals("!=")) {
						LinkedHashMap<String, Object> temp = new LinkedHashMap<String, Object>();
						temp.put(sqlTerm._strColumnName, sqlTerm._objValue);
						if (!Equals(temp, p.arrRecords.get(j)))
							result.add(p.arrRecords.get(j));

					} else if (sqlTerm._strOperator.equals("=")) {
						LinkedHashMap<String, Object> temp = new LinkedHashMap<String, Object>();
						temp.put(sqlTerm._strColumnName, sqlTerm._objValue);
						if (Equals(temp, p.arrRecords.get(j)))
							result.add(p.arrRecords.get(j));

					}

				}

			}

		}
		return result;
	}

	public Vector<LinkedHashMap<String, Object>> FilterIndex(SQLTerm sqlTerm) throws DBAppException {

		Table RequestedTable = (Table) DeSerialize("data/" + sqlTerm._strTableName + ".ser");
		Vector<LinkedHashMap<String, Object>> result = new Vector<LinkedHashMap<String, Object>>();

		if (RequestedTable.htblColNameType.get(sqlTerm._strColumnName).equals("java.awt.Polygon")) {
			RTree Index = (RTree) DeSerialize(
					"data/" + sqlTerm._strTableName + "IndexOn" + sqlTerm._strColumnName + ".ser");

			switch (sqlTerm._strOperator) {
			case "=":
				if (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
					Vector<int[]> vec = Index.search((Comparable) sqlTerm._objValue);
					for (int i = 0; i < vec.size(); i++) {
						Page p = (Page) DeSerialize(
								"data" + "/" + RequestedTable.strTableName + "" + vec.get(i)[0] + ".ser");
						if (i == 0) {
							int z = FindBinarySearch(p.arrRecords, RequestedTable.strClusteringKeyColumn, 0,
									p.arrRecords.size(), sqlTerm._objValue);
							for (; z > 0; z--) {
								if (compareTo(p.arrRecords.get(z - 1).get(sqlTerm._strColumnName),
										sqlTerm._objValue) != 0)
									break;
							}
							for (int j = z; j < p.arrRecords.size(); j++) {
								if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) == 0)
									result.add(p.arrRecords.get(j));
								else
									return result;
							}
						} else {
							// if p.arrRecords.get(j).get(sqlTerm._strColumnName)==max of page then add all
							for (int j = 0; j < p.arrRecords.size(); j++) {

								if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) == 0)
									result.add(p.arrRecords.get(j));
								else
									return result;
							}
						}
					}
				} else {
					Vector<int[]> vec = Index.search((Comparable) sqlTerm._objValue);
					for (int i = 0; i < vec.size(); i++) {
						Page p = (Page) DeSerialize(
								"data" + "/" + RequestedTable.strTableName + "" + vec.get(i)[0] + ".ser");
						for (int j = 0; j < p.arrRecords.size(); j++) {
							if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) == 0)
								result.add(p.arrRecords.get(j));
						}
					}
				}
				break;
			case ">":
				if (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
					for (int i = RequestedTable.pages; i >= 1; i--) {

						Page p = (Page) DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
						if (compareTo(sqlTerm._objValue, RequestedTable.Ranges.get(i - 1)[0]) < 0) {
							result.addAll(p.arrRecords);
						} else {
							for (int j = p.arrRecords.size() - 1; j >= 0; j--) {
								if (compareTo(sqlTerm._objValue, p.arrRecords.get(j).get(sqlTerm._strColumnName)) < 0) {
									result.add(p.arrRecords.get(j));
								} else
									return result;
							}
						}
					}
				} else {
					// no clustered range using index
					List<Vector<int[]>> l = Index.searchBiggerThan((Comparable) sqlTerm._objValue,
							Team_Barso.RTree.RangePolicy.EXCLUSIVE);
					ArrayList<Integer> pages = unifyPages(l);
					for (int j = 0; j < pages.size(); j++) {
						Page p = (Page) DeSerialize(
								"data" + "/" + RequestedTable.strTableName + "" + pages.get(j) + ".ser");
						for (int z = 0; z < p.arrRecords.size(); z++) {
							if (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName), sqlTerm._objValue) > 0)
								result.add(p.arrRecords.get(z));
						}
					}
				}
				break;
			case ">=":
				if (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
					for (int i = RequestedTable.pages; i >= 1; i--) {
						Page p = (Page) DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
						if (compareTo(sqlTerm._objValue, RequestedTable.Ranges.get(i - 1)[0]) <= 0) {
							result.addAll(p.arrRecords);
						} else {
							for (int j = p.arrRecords.size() - 1; j >= 0; j--) {
								if (compareTo(sqlTerm._objValue,
										p.arrRecords.get(j).get(sqlTerm._strColumnName)) <= 0) {
									result.add(p.arrRecords.get(j));
								} else
									return result;
							}
						}
					}
				} else {
					// no clustered range using index
					List<Vector<int[]>> l = Index.searchBiggerThan((Comparable) sqlTerm._objValue,
							Team_Barso.RTree.RangePolicy.INCLUSIVE);
					ArrayList<Integer> pages = unifyPages(l);
					for (int j = 0; j < pages.size(); j++) {
						Page p = (Page) DeSerialize(
								"data" + "/" + RequestedTable.strTableName + "" + pages.get(j) + ".ser");
						for (int z = 0; z < p.arrRecords.size(); z++) {
							if (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName), sqlTerm._objValue) >= 0)
								result.add(p.arrRecords.get(z));
						}
					}
				}
				break;
			case "<":
				if (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {

					for (int i = 1; i <= RequestedTable.pages; i++) {
						Page p = (Page) DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
						for (int j = 0; j < p.arrRecords.size(); j++) {
							if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) < 0)
								result.add(p.arrRecords.get(j));
							else
								return result;
						}
					}
				} else {
					// no clustered range using index
					List<Vector<int[]>> l = Index.searchLessThan((Comparable) sqlTerm._objValue,
							Team_Barso.RTree.RangePolicy.EXCLUSIVE);
					ArrayList<Integer> pages = unifyPages(l);
					for (int j = 0; j < pages.size(); j++) {
						Page p = (Page) DeSerialize(
								"data" + "/" + RequestedTable.strTableName + "" + pages.get(j) + ".ser");
						for (int z = 0; z < p.arrRecords.size(); z++) {
							if (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName), sqlTerm._objValue) < 0)
								result.add(p.arrRecords.get(z));
						}
					}

				}
				break;
			case "<=":
				if (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {

					for (int i = 1; i <= RequestedTable.pages; i++) {
						Page p = (Page) DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
						for (int j = 0; j < p.arrRecords.size(); j++) {
							if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) <= 0)
								result.add(p.arrRecords.get(j));
							else
								return result;
						}
					}
				} else {
					// no clustered range using index
					List<Vector<int[]>> l = Index.searchLessThan((Comparable) sqlTerm._objValue,
							Team_Barso.RTree.RangePolicy.INCLUSIVE);
					ArrayList<Integer> pages = unifyPages(l);
					for (int j = 0; j < pages.size(); j++) {
						Page p = (Page) DeSerialize(
								"data" + "/" + RequestedTable.strTableName + "" + pages.get(j) + ".ser");
						for (int z = 0; z < p.arrRecords.size(); z++) {
							if (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName), sqlTerm._objValue) <= 0)
								result.add(p.arrRecords.get(z));
						}
					}

				}
				break;
			case "!=":
				if (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {

					for (int i = 1; i <= RequestedTable.pages; i++) {
						Page p = (Page) DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");

						if (compareTo(sqlTerm._objValue, RequestedTable.Ranges.get(i - 1)[0]) >= 0
								&& compareTo(sqlTerm._objValue, RequestedTable.Ranges.get(i - 1)[1]) <= 0) {

							for (int j = 0; j < p.arrRecords.size(); j++) {
								if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) != 0)
									result.add(p.arrRecords.get(j));
							}
						} else {
							result.addAll(p.arrRecords);
						}
					}
				} else {
					for (int i = 1; i <= RequestedTable.pages; i++) {
						Page p = (Page) DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
						for (int j = 0; j < p.arrRecords.size(); j++) {
							if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) != 0)
								result.add(p.arrRecords.get(j));
						}
					}
				}
				break;
			default:
				throw new DBAppException("sqlOperater undefined");
			}

		} else {
			BPlusTree Index = (BPlusTree) DeSerialize(
					"data/" + sqlTerm._strTableName + "IndexOn" + sqlTerm._strColumnName + ".ser");
			switch (sqlTerm._strOperator) {
			case "=":
				if (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
					Vector<int[]> vec = Index.search((Comparable) sqlTerm._objValue);
					for (int i = 0; i < vec.size(); i++) {
						Page p = (Page) DeSerialize(
								"data" + "/" + RequestedTable.strTableName + "" + vec.get(i)[0] + ".ser");
						if (i == 0) {
							int z = FindBinarySearch(p.arrRecords, RequestedTable.strClusteringKeyColumn, 0,
									p.arrRecords.size(), sqlTerm._objValue);
							for (; z > 0; z--) {

								if (compareTo(p.arrRecords.get(z - 1).get(sqlTerm._strColumnName),
										sqlTerm._objValue) != 0)
									break;
							}
							for (int j = z; j < p.arrRecords.size(); j++) {

								if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) == 0)
									result.add(p.arrRecords.get(j));
								else
									return result;
							}
						} else {
							for (int j = 0; j < p.arrRecords.size(); j++) {

								if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) == 0)
									result.add(p.arrRecords.get(j));
								else
									return result;
							}
						}
					}
				} else {
					Vector<int[]> vec = Index.search((Comparable) sqlTerm._objValue);
					for (int i = 0; i < vec.size(); i++) {
						Page p = (Page) DeSerialize(
								"data" + "/" + RequestedTable.strTableName + "" + vec.get(i)[0] + ".ser");
						for (int j = 0; j < p.arrRecords.size(); j++) {
							if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) == 0)
								result.add(p.arrRecords.get(j));
						}
					}
				}
				break;
			case ">":
				if (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
					for (int i = RequestedTable.pages; i >= 1; i--) {
						Page p = (Page) DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
						if (compareTo(sqlTerm._objValue, RequestedTable.Ranges.get(i - 1)[0]) < 0) {
							result.addAll(p.arrRecords);
						} else {
							for (int j = p.arrRecords.size() - 1; j >= 0; j--) {
								if (compareTo(sqlTerm._objValue, p.arrRecords.get(j).get(sqlTerm._strColumnName)) < 0) {
									result.add(p.arrRecords.get(j));
								} else
									return result;
							}
						}
					}
				} else {
					// no clustered range using index
					List<Vector<int[]>> l = Index.searchBiggerThan((Comparable) sqlTerm._objValue,
							RangePolicy.EXCLUSIVE);
					ArrayList<Integer> pages = unifyPages(l);
					for (int j = 0; j < pages.size(); j++) {
						Page p = (Page) DeSerialize(
								"data" + "/" + RequestedTable.strTableName + "" + pages.get(j) + ".ser");
						for (int z = 0; z < p.arrRecords.size(); z++) {
							if (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName), sqlTerm._objValue) > 0)
								result.add(p.arrRecords.get(z));
						}
					}
				}
				break;
			case ">=":
				if (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
					for (int i = RequestedTable.pages; i >= 1; i--) {
						Page p = (Page) DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
						if (compareTo(sqlTerm._objValue, RequestedTable.Ranges.get(i - 1)[0]) <= 0) {
							result.addAll(p.arrRecords);
						} else {
							for (int j = p.arrRecords.size() - 1; j >= 0; j--) {
								if (compareTo(sqlTerm._objValue,
										p.arrRecords.get(j).get(sqlTerm._strColumnName)) <= 0) {
									result.add(p.arrRecords.get(j));
								} else
									return result;
							}
						}
					}
				} else {
					// no clustered range using index
					List<Vector<int[]>> l = Index.searchBiggerThan((Comparable) sqlTerm._objValue,
							RangePolicy.INCLUSIVE);
					ArrayList<Integer> pages = unifyPages(l);
					for (int j = 0; j < pages.size(); j++) {
						Page p = (Page) DeSerialize(
								"data" + "/" + RequestedTable.strTableName + "" + pages.get(j) + ".ser");
						for (int z = 0; z < p.arrRecords.size(); z++) {
							if (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName), sqlTerm._objValue) >= 0)
								result.add(p.arrRecords.get(z));
						}
					}
				}
				break;
			case "<":
				if (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {

					for (int i = 1; i <= RequestedTable.pages; i++) {
						Page p = (Page) DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
						for (int j = 0; j < p.arrRecords.size(); j++) {
							if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) < 0)
								result.add(p.arrRecords.get(j));
							else
								return result;
						}
					}
				} else {
					// no clustered range using index
					List<Vector<int[]>> l = Index.searchLessThan((Comparable) sqlTerm._objValue, RangePolicy.EXCLUSIVE);
					ArrayList<Integer> pages = unifyPages(l);
					for (int j = 0; j < pages.size(); j++) {
						Page p = (Page) DeSerialize(
								"data" + "/" + RequestedTable.strTableName + "" + pages.get(j) + ".ser");
						for (int z = 0; z < p.arrRecords.size(); z++) {
							if (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName), sqlTerm._objValue) < 0)
								result.add(p.arrRecords.get(z));
						}
					}

				}
				break;
			case "<=":
				if (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {

					for (int i = 1; i <= RequestedTable.pages; i++) {
						Page p = (Page) DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
						for (int j = 0; j < p.arrRecords.size(); j++) {
							if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) <= 0)
								result.add(p.arrRecords.get(j));
							else
								return result;
						}
					}
				} else {
					// no clustered range using index
					List<Vector<int[]>> l = Index.searchLessThan((Comparable) sqlTerm._objValue, RangePolicy.INCLUSIVE);
					ArrayList<Integer> pages = unifyPages(l);
					for (int j = 0; j < pages.size(); j++) {
						Page p = (Page) DeSerialize(
								"data" + "/" + RequestedTable.strTableName + "" + pages.get(j) + ".ser");
						for (int z = 0; z < p.arrRecords.size(); z++) {
							if (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName), sqlTerm._objValue) <= 0)
								result.add(p.arrRecords.get(z));
						}
					}

				}
				break;
			case "!=":
				if (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {

					for (int i = 1; i <= RequestedTable.pages; i++) {
						Page p = (Page) DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");

						if (compareTo(sqlTerm._objValue, RequestedTable.Ranges.get(i - 1)[0]) >= 0
								&& compareTo(sqlTerm._objValue, RequestedTable.Ranges.get(i - 1)[1]) <= 0) {

							for (int j = 0; j < p.arrRecords.size(); j++) {
								if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) != 0)
									result.add(p.arrRecords.get(j));
							}
						} else {
							result.addAll(p.arrRecords);
						}
					}
				} else {
					for (int i = 1; i <= RequestedTable.pages; i++) {
						Page p = (Page) DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
						for (int j = 0; j < p.arrRecords.size(); j++) {
							if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName), sqlTerm._objValue) != 0)
								result.add(p.arrRecords.get(j));
						}
					}
				}
				break;
			default:
				throw new DBAppException("sqlOperater undefined");
			}
		}

		return result;
	}

	private Vector<LinkedHashMap<String, Object>> IncludeRestofTable(int i, String strTableName,
			Vector<LinkedHashMap<String, Object>> result) {
		Table RequestedTable = (Table) DeSerialize("data/" + strTableName + ".ser");
		for (i = i; i <= RequestedTable.pages; i++) {
			Page p = (Page) DeSerialize("data/" + strTableName + i + ".ser");
			result.addAll(p.arrRecords);
		}

		return result;
	}

	public Vector<LinkedHashMap<String, Object>> AND(Vector<LinkedHashMap<String, Object>> Set1,
			Vector<LinkedHashMap<String, Object>> Set2) {

		Vector<LinkedHashMap<String, Object>> Result = new Vector<LinkedHashMap<String, Object>>();
		if (Set1.size() <= Set2.size())
			Set1.forEach((k) -> {
				if (Set2.contains(k) && !Result.contains(k)) {
					Result.add(k);
				}

			});
		else {
			Set2.forEach((k) -> {
				if (Set1.contains(k) && !Result.contains(k)) {
					Result.add(k);
				}

			});
		}

		return Result;

	}

	public Vector<LinkedHashMap<String, Object>> OR(Vector<LinkedHashMap<String, Object>> Set1,
			Vector<LinkedHashMap<String, Object>> Set2) {

		Vector<LinkedHashMap<String, Object>> Result = new Vector<LinkedHashMap<String, Object>>();

		if (Set1.size() >= Set2.size()) {
			Result.addAll(Set1);
			Set2.forEach((k) -> {
				if (!Result.contains(k)) {
					Result.add(k);
				}

			});

		} else {

			Result.addAll(Set2);
			Set1.forEach((k) -> {
				if (!Result.contains(k)) {
					Result.add(k);
				}

			});
		}

		return Result;

	}

	public Vector<LinkedHashMap<String, Object>> XOR(Vector<LinkedHashMap<String, Object>> Set1,
			Vector<LinkedHashMap<String, Object>> Set2) {

		Vector<LinkedHashMap<String, Object>> Result = new Vector<LinkedHashMap<String, Object>>();
		for (int i = 0; i < Set1.size(); i++) {
			LinkedHashMap<String, Object> k = Set1.get(i);
			boolean found2 = Set2.contains(k);
			if (!found2) {
				Result.add(k);
			} else {
				Set2.remove(k);
			}
		}
		Result.addAll(Set2);

		return Result;

	}

	private Vector<LinkedHashMap<String, Object>> checkNextPage(int i, String strTableName, String strColumnName,
			Object _objValue, Vector<LinkedHashMap<String, Object>> result) {
		// TODO Auto-generated method stub
		Table RequestedTable = (Table) DeSerialize("data/" + strTableName + ".ser");
		boolean includerest = false;
		for (i = i; i <= RequestedTable.pages; i++) {
			Page p = (Page) DeSerialize("data/" + strTableName + ".ser");
			if (includerest)
				result.addAll(p.arrRecords);
			else
				for (int j = 0; j < p.arrRecords.size(); j++) {
					if (compareTo(_objValue, p.arrRecords.get(j).get(strColumnName)) < 0) {
						includerest = true;
						result.addAll(result.subList(j, p.arrRecords.size()));

					}

				}

		}

		return result;
	}

	/*
	 * public Vector<LinkedHashMap<String, Object>> FilterIndex(SQLTerm sqlTerm,
	 * Table RequestedTable) throws DBAppException {
	 * 
	 * Vector<LinkedHashMap<String, Object>> result = new
	 * Vector<LinkedHashMap<String, Object>>();
	 * 
	 * if (RequestedTable.htblColNameType.get(sqlTerm._strColumnName).equals(
	 * "java.awt.Polygon")) { RTree Index = (RTree) DeSerialize( "data/" +
	 * sqlTerm._strTableName + "IndexOn" + sqlTerm._strColumnName + ".ser");
	 * 
	 * switch (sqlTerm._strOperator) { case "=": if
	 * (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
	 * Vector<int[]> vec = Index.search((Comparable) sqlTerm._objValue); for (int i
	 * = 0; i < vec.size(); i++) { Page p = (Page) DeSerialize( "data" + "/" +
	 * RequestedTable.strTableName + "" + vec.get(i)[0] + ".ser"); if (i == 0) { int
	 * z = FindBinarySearch(p.arrRecords, RequestedTable.strClusteringKeyColumn, 0,
	 * p.arrRecords.size(), sqlTerm._objValue); for (int j = z; j >= 0; j--) {
	 * 
	 * if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) == 0) result.add(p.arrRecords.get(j)); else return result;
	 * } } else { for (int j = 0; j < p.arrRecords.size(); j++) {
	 * 
	 * if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) == 0) result.add(p.arrRecords.get(j)); else return result;
	 * } } } } else { Vector<int[]> vec = Index.search((Comparable)
	 * sqlTerm._objValue); for (int i = 0; i < vec.size(); i++) { Page p = (Page)
	 * DeSerialize( "data" + "/" + RequestedTable.strTableName + "" + vec.get(i)[0]
	 * + ".ser"); for (int j = 0; j < p.arrRecords.size(); j++) { if
	 * (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) == 0) result.add(p.arrRecords.get(j)); } } } break; case
	 * ">": if
	 * (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) { for
	 * (int i = RequestedTable.pages; i >= 1; i--) {
	 * 
	 * Page p = (Page) DeSerialize("data" + "/" + RequestedTable.strTableName + "" +
	 * i + ".ser"); if (compareTo(sqlTerm._objValue, RequestedTable.Ranges.get()) <
	 * 0) { result.addAll(p.arrRecords); } else { for (int j = p.arrRecords.size();
	 * j >= 0; j--) { if (compareTo(sqlTerm._objValue,
	 * p.arrRecords.get(j).get(sqlTerm._strColumnName)) < 0) {
	 * result.add(p.arrRecords.get(j)); } else return result; } } } } else { // no
	 * clustered range using index List<Vector<int[]>> l =
	 * Index.searchBiggerThan((Comparable) sqlTerm._objValue,
	 * Team_Barso.RTree.RangePolicy.EXCLUSIVE); ArrayList<Integer> pages =
	 * unifyPages(l); for (int j = 0; j < pages.size(); j++) { Page p = (Page)
	 * DeSerialize( "data" + "/" + RequestedTable.strTableName + "" + pages.get(j) +
	 * ".ser"); for (int z = 0; z < p.arrRecords.size(); z++) { if
	 * (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) > 0) result.add(p.arrRecords.get(z)); } } } break; case
	 * ">=": if
	 * (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) { for
	 * (int i = RequestedTable.pages; i >= 1; i++) { Page p = (Page)
	 * DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser"); if
	 * (compareTo(sqlTerm._objValue, p.Min) <= 0) { result.addAll(p.arrRecords); }
	 * else { for (int j = p.arrRecords.size(); j >= 0; j--) { if
	 * (compareTo(sqlTerm._objValue,
	 * p.arrRecords.get(j).get(sqlTerm._strColumnName)) <= 0) {
	 * result.add(p.arrRecords.get(j)); } else return result; } } } } else { // no
	 * clustered range using index List<Vector<int[]>> l =
	 * Index.searchBiggerThan((Comparable) sqlTerm._objValue,
	 * Team_Barso.RTree.RangePolicy.INCLUSIVE); ArrayList<Integer> pages =
	 * unifyPages(l); for (int j = 0; j < pages.size(); j++) { Page p = (Page)
	 * DeSerialize( "data" + "/" + RequestedTable.strTableName + "" + pages.get(j) +
	 * ".ser"); for (int z = 0; z < p.arrRecords.size(); z++) { if
	 * (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) >= 0) result.add(p.arrRecords.get(z)); } } } break; case
	 * "<": if
	 * (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
	 * 
	 * for (int i = 1; i <= RequestedTable.pages; i++) { Page p = (Page)
	 * DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
	 * for (int j = 0; j < p.arrRecords.size(); j++) { if
	 * (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) < 0) result.add(p.arrRecords.get(j)); else return result;
	 * } } } else { // no clustered range using index List<Vector<int[]>> l =
	 * Index.searchLessThan((Comparable) sqlTerm._objValue,
	 * Team_Barso.RTree.RangePolicy.EXCLUSIVE); ArrayList<Integer> pages =
	 * unifyPages(l); for (int j = 0; j < pages.size(); j++) { Page p = (Page)
	 * DeSerialize( "data" + "/" + RequestedTable.strTableName + "" + pages.get(j) +
	 * ".ser"); for (int z = 0; z < p.arrRecords.size(); z++) { if
	 * (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) < 0) result.add(p.arrRecords.get(z)); } }
	 * 
	 * } break; case "<=": if
	 * (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
	 * 
	 * for (int i = 1; i <= RequestedTable.pages; i++) { Page p = (Page)
	 * DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
	 * for (int j = 0; j < p.arrRecords.size(); j++) { if
	 * (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) <= 0) result.add(p.arrRecords.get(j)); else return result;
	 * } } } else { // no clustered range using index List<Vector<int[]>> l =
	 * Index.searchLessThan((Comparable) sqlTerm._objValue,
	 * Team_Barso.RTree.RangePolicy.INCLUSIVE); ArrayList<Integer> pages =
	 * unifyPages(l); for (int j = 0; j < pages.size(); j++) { Page p = (Page)
	 * DeSerialize( "data" + "/" + RequestedTable.strTableName + "" + pages.get(j) +
	 * ".ser"); for (int z = 0; z < p.arrRecords.size(); z++) { if
	 * (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) <= 0) result.add(p.arrRecords.get(z)); } }
	 * 
	 * } break; case "!=": if
	 * (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
	 * 
	 * for (int i = 1; i <= RequestedTable.pages; i++) { Page p = (Page)
	 * DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
	 * 
	 * if (compareTo(sqlTerm._objValue, p.Min) >= 0 && compareTo(sqlTerm._objValue,
	 * p.Max) <= 0) {
	 * 
	 * for (int j = 0; j < p.arrRecords.size(); j++) { if
	 * (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) != 0) result.add(p.arrRecords.get(j)); } }else {
	 * result.addAll(p.arrRecords); } } } else { for (int i = 1; i <=
	 * RequestedTable.pages; i++) { Page p = (Page) DeSerialize("data" + "/" +
	 * RequestedTable.strTableName + "" + i + ".ser"); for (int j = 0; j <
	 * p.arrRecords.size(); j++) { if
	 * (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) != 0) result.add(p.arrRecords.get(j)); } } } break;
	 * default:throw new DBAppException("sqlOperater undefined"); }
	 * 
	 * } else { BPlusTree Index = (BPlusTree) DeSerialize( "data/" +
	 * sqlTerm._strTableName + "IndexOn" + sqlTerm._strColumnName + ".ser"); switch
	 * (sqlTerm._strOperator) { case "=": if
	 * (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
	 * Vector<int[]> vec = Index.search((Comparable) sqlTerm._objValue); for (int i
	 * = 0; i < vec.size(); i++) { Page p = (Page) DeSerialize( "data" + "/" +
	 * RequestedTable.strTableName + "" + vec.get(i)[0] + ".ser"); if (i == 0) { int
	 * z = FindBinarySearch(p.arrRecords, RequestedTable.strClusteringKeyColumn, 0,
	 * p.arrRecords.size(), sqlTerm._objValue); for (int j = z; j >= 0; j--) {
	 * 
	 * if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) == 0) result.add(p.arrRecords.get(j)); else return result;
	 * } } else { for (int j = 0; j < p.arrRecords.size(); j++) {
	 * 
	 * if (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) == 0) result.add(p.arrRecords.get(j)); else return result;
	 * } } } } else { Vector<int[]> vec = Index.search((Comparable)
	 * sqlTerm._objValue); for (int i = 0; i < vec.size(); i++) { Page p = (Page)
	 * DeSerialize( "data" + "/" + RequestedTable.strTableName + "" + vec.get(i)[0]
	 * + ".ser"); for (int j = 0; j < p.arrRecords.size(); j++) { if
	 * (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) == 0) result.add(p.arrRecords.get(j)); } } } break; case
	 * ">": if
	 * (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) { for
	 * (int i = RequestedTable.pages; i >= 1; i--) { Page p = (Page)
	 * DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser"); if
	 * (compareTo(sqlTerm._objValue, p.Min) < 0) { result.addAll(p.arrRecords); }
	 * else { for (int j = p.arrRecords.size(); j >= 0; j--) { if
	 * (compareTo(sqlTerm._objValue,
	 * p.arrRecords.get(j).get(sqlTerm._strColumnName)) < 0) {
	 * result.add(p.arrRecords.get(j)); } else return result; } } } } else { // no
	 * clustered range using index List<Vector<int[]>> l =
	 * Index.searchBiggerThan((Comparable) sqlTerm._objValue,RangePolicy.EXCLUSIVE);
	 * ArrayList<Integer> pages = unifyPages(l); for (int j = 0; j < pages.size();
	 * j++) { Page p = (Page) DeSerialize( "data" + "/" +
	 * RequestedTable.strTableName + "" + pages.get(j) + ".ser"); for (int z = 0; z
	 * < p.arrRecords.size(); z++) { if
	 * (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) > 0) result.add(p.arrRecords.get(z)); } } } break; case
	 * ">=": if
	 * (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) { for
	 * (int i = RequestedTable.pages; i >= 1; i++) { Page p = (Page)
	 * DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser"); if
	 * (compareTo(sqlTerm._objValue, p.Min) <= 0) { result.addAll(p.arrRecords); }
	 * else { for (int j = p.arrRecords.size(); j >= 0; j--) { if
	 * (compareTo(sqlTerm._objValue,
	 * p.arrRecords.get(j).get(sqlTerm._strColumnName)) <= 0) {
	 * result.add(p.arrRecords.get(j)); } else return result; } } } } else { // no
	 * clustered range using index List<Vector<int[]>> l =
	 * Index.searchBiggerThan((Comparable) sqlTerm._objValue,RangePolicy.INCLUSIVE);
	 * ArrayList<Integer> pages = unifyPages(l); for (int j = 0; j < pages.size();
	 * j++) { Page p = (Page) DeSerialize( "data" + "/" +
	 * RequestedTable.strTableName + "" + pages.get(j) + ".ser"); for (int z = 0; z
	 * < p.arrRecords.size(); z++) { if
	 * (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) >= 0) result.add(p.arrRecords.get(z)); } } } break; case
	 * "<": if
	 * (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
	 * 
	 * for (int i = 1; i <= RequestedTable.pages; i++) { Page p = (Page)
	 * DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
	 * for (int j = 0; j < p.arrRecords.size(); j++) { if
	 * (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) < 0) result.add(p.arrRecords.get(j)); else return result;
	 * } } } else { // no clustered range using index List<Vector<int[]>> l =
	 * Index.searchLessThan((Comparable) sqlTerm._objValue,RangePolicy.EXCLUSIVE);
	 * ArrayList<Integer> pages = unifyPages(l); for (int j = 0; j < pages.size();
	 * j++) { Page p = (Page) DeSerialize( "data" + "/" +
	 * RequestedTable.strTableName + "" + pages.get(j) + ".ser"); for (int z = 0; z
	 * < p.arrRecords.size(); z++) { if
	 * (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) < 0) result.add(p.arrRecords.get(z)); } }
	 * 
	 * } break; case "<=": if
	 * (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
	 * 
	 * for (int i = 1; i <= RequestedTable.pages; i++) { Page p = (Page)
	 * DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
	 * for (int j = 0; j < p.arrRecords.size(); j++) { if
	 * (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) <= 0) result.add(p.arrRecords.get(j)); else return result;
	 * } } } else { // no clustered range using index List<Vector<int[]>> l =
	 * Index.searchLessThan((Comparable) sqlTerm._objValue,RangePolicy.INCLUSIVE);
	 * ArrayList<Integer> pages = unifyPages(l); for (int j = 0; j < pages.size();
	 * j++) { Page p = (Page) DeSerialize( "data" + "/" +
	 * RequestedTable.strTableName + "" + pages.get(j) + ".ser"); for (int z = 0; z
	 * < p.arrRecords.size(); z++) { if
	 * (compareTo(p.arrRecords.get(z).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) <= 0) result.add(p.arrRecords.get(z)); } }
	 * 
	 * } break; case "!=": if
	 * (sqlTerm._strColumnName.equals(RequestedTable.strClusteringKeyColumn)) {
	 * 
	 * for (int i = 1; i <= RequestedTable.pages; i++) { Page p = (Page)
	 * DeSerialize("data" + "/" + RequestedTable.strTableName + "" + i + ".ser");
	 * 
	 * if (compareTo(sqlTerm._objValue, p.Min) >= 0 && compareTo(sqlTerm._objValue,
	 * p.Max) <= 0) {
	 * 
	 * for (int j = 0; j < p.arrRecords.size(); j++) { if
	 * (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) != 0) result.add(p.arrRecords.get(j)); } }else {
	 * result.addAll(p.arrRecords); } } } else { for (int i = 1; i <=
	 * RequestedTable.pages; i++) { Page p = (Page) DeSerialize("data" + "/" +
	 * RequestedTable.strTableName + "" + i + ".ser"); for (int j = 0; j <
	 * p.arrRecords.size(); j++) { if
	 * (compareTo(p.arrRecords.get(j).get(sqlTerm._strColumnName),
	 * sqlTerm._objValue) != 0) result.add(p.arrRecords.get(j)); } } } break;
	 * default:throw new DBAppException("sqlOperater undefined"); } }
	 * 
	 * return result; }
	 */
	public static ArrayList<Integer> unifyPages(List<Vector<int[]>> l) {
		ArrayList<Integer> result = new ArrayList<Integer>();
		for (int i = 0; i < l.size(); i++) {
			Vector<int[]> Key = l.get(i);
			for (int j = 0; j < Key.size(); j++) {
				if (!result.contains(Key.get(j)[0]))
					result.add(Key.get(j)[0]);
			}
		}
		return result;
	}

	public static Double getArea(ProcessPolygon p) {

		return ((Double) (p.getBounds().getSize().getHeight() * p.getBounds().getSize().getWidth()));
	}

	public static void PopulateRandom(int maxID, String strTableName, int numRecords, DBApp app)
			throws IOException, ParseException, DBAppException {

		Table RequestedTable = (Table) DeSerialize("data/" + strTableName + ".ser");
		String[] names = null;
		String row = "";
		BufferedReader csvReader = new BufferedReader(new FileReader("names.csv"));

		while ((row = csvReader.readLine()) != null) {
			names = row.split(",");
			// do something with the data
		}
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		dateFormat.setLenient(false);
		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		Random random = new Random();
		for (int i = 0; i < numRecords; i++) {
			int year = randBetween(1997, 2001);
			int day = randBetween(2, 28);
			int month = randBetween(2, 12);
			Date date = dateFormat.parse(year + "-" + month + "-" + day);
			int id = random.nextInt(maxID);
			int dim = random.nextInt(20);
			Polygon Home = new Polygon();
			int name = random.nextInt(names.length);
			Home.addPoint(random.nextInt(5) + 1, random.nextInt(5) + 1);
			Home.addPoint(random.nextInt(5) + 1, random.nextInt(5) + 1);
			Home.addPoint(random.nextInt(5) + 1, random.nextInt(5) + 1);
			Home.addPoint(random.nextInt(5) + 1, random.nextInt(5) + 1);
			htblColNameValue.put("id", id);
			htblColNameValue.put("name", names[name]);

			if (i % 6 == 0)
				htblColNameValue.put("gpa", 1.5);

			else
				htblColNameValue.put("gpa", 2.5);

			htblColNameValue.put("Birthday", date);
			htblColNameValue.put("activated", true);
			htblColNameValue.put("Home", Home);
			app.insertIntoTable("Student", htblColNameValue);
			htblColNameValue.clear();

		}
	}

	public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException, ParseException {

		
		  DBApp dbApp = new DBApp(); dbApp.init(); Long startTime = System.nanoTime();
		  SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		  dateFormat.setLenient(false); Random random = new Random(); ProcessPolygon x
		  = new ProcessPolygon(); Hashtable htblColNameType = new Hashtable();
		  htblColNameType.put("id", Integer.class.getCanonicalName());
		  htblColNameType.put("name", String.class.getCanonicalName());
		  htblColNameType.put("gpa", Double.class.getCanonicalName());
		  htblColNameType.put("Birthday", Date.class.getCanonicalName());
		  htblColNameType.put("activated", Boolean.class.getCanonicalName());
		  htblColNameType.put("Home", Polygon.class.getCanonicalName());
		  dbApp.createTable("Student", "id", htblColNameType);
		  dbApp.createBTreeIndex("Student", "id");
		  dbApp.PopulateRandom(150, "Student", 3000, dbApp);
		  
			/*
			 * SQLTerm[] arrSQLTerms = new SQLTerm[2]; String[] strarrOperators = new
			 * String[1];
			 * 
			 * arrSQLTerms[0]._strColumnName = "id"; arrSQLTerms[0]._strTableName =
			 * "Student"; arrSQLTerms[0]._strOperator = "="; arrSQLTerms[0]._objValue = 6;
			 * 
			 * arrSQLTerms[1] = new SQLTerm(); arrSQLTerms[1]._strColumnName = "id";
			 * arrSQLTerms[1]._strTableName = "Student"; arrSQLTerms[1]._strOperator = "=";
			 * arrSQLTerms[1]._objValue = 7;
			 * 
			 * strarrOperators[0] = "OR";
			 * 
			 * Iterator result = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
			 * 
			 * Table Student = (Table) DeSerialize("data/Student.ser");
			 * Student.DisplayTable();
			 * 
			 * while (result.hasNext()) { System.out.println(result.next()); }
			 */
		 
	}

}