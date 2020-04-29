package Team_Barso;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.Vector;

public class BPlusTree<K extends Comparable<? super K>, V> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1886674299583241987L;

	public static enum RangePolicy {
		EXCLUSIVE, INCLUSIVE
	}

	/**
	 * The branching factor used when none specified in constructor.
	 */
	private static final int DEFAULT_BRANCHING_FACTOR = 128;

	/**
	 * The branching factor for the B+ tree, that measures the capacity of nodes
	 * (i.e., the number of children nodes) for internal nodes in the tree.
	 */
	private int branchingFactor;

	/**
	 * The root node of the B+ tree.
	 */
	// static String root;
	String tableName;
	String columnName;
	int IndexNum;
	//private static final long serialVersionUID;
	// public static int numNodes;

	public Node getRoot() {
		RootNode r = (RootNode) DBApp.DeSerialize("data/" + tableName + IndexNum + "ROOT" + ".ser");
		Node n1 = (Node) DBApp.DeSerialize(r.Path);
		return n1;
	}

	public BPlusTree(String tableName, String columnName, int IndexNum) {
		this(DEFAULT_BRANCHING_FACTOR, tableName, columnName, IndexNum);
	}

	public BPlusTree(int branchingFactor, String tableName, String columnName, int IndexNum) {
		if (branchingFactor <= 2)
			throw new IllegalArgumentException("Illegal branching factor: " + branchingFactor);
		this.branchingFactor = branchingFactor;
		this.tableName = tableName;
		this.columnName = columnName;
		this.IndexNum = IndexNum;
		LeafNode n = new LeafNode();
		DBApp.serialize(n.nodeName, n);
		RootNode r = new RootNode(n.nodeName);
		DBApp.serialize("data/" + tableName + IndexNum + "ROOT" + ".ser", r);
		// root = n.nodeName;
	}

	public void UpdateNode(K key, int oldpage, int newpage) {

		RootNode r = (RootNode) DBApp.DeSerialize("data/" + tableName + IndexNum + "ROOT" + ".ser");
		Node n1 = (Node) DBApp.DeSerialize(r.Path);
		LeafNode toChange = (LeafNode) n1.getNode(key);
		int loc = Collections.binarySearch(toChange.keys, key);
		if (loc >= 0) {
			boolean found = false;
			Vector<int[]> vec = toChange.values.get(loc);
			int[] tupleAtLoc;
			for (int i = 0; i < vec.size(); i++) {
				tupleAtLoc = vec.get(i);
				if (tupleAtLoc[0] == oldpage) {
					found = true;
					tupleAtLoc[1]--;
					if (tupleAtLoc[1] == 0)
						vec.remove(i);
					break;
				}
			}
			if (found) {
				for (int j = 0; j < vec.size(); j++) {
					tupleAtLoc = vec.get(j);
					if (tupleAtLoc[0] == newpage) {
						tupleAtLoc[1]++;
						DBApp.serialize(toChange.nodeName, toChange);
						System.out.println(
								"serialized in method UpdateNode(" + key + ") in Class Node:" + toChange + "\n");
						System.out.println("Node Update Success");
						return;
					}
				}				
				boolean added= false;
				int[] tuple = { newpage, 1 };
				for(int i=0 ; i<vec.size() ; i++) {
					if(vec.get(i)[0]>newpage) {
						vec.add(i, tuple);
						added=true;
						break;
					}
				}
				if(!added)vec.add(vec.size(),tuple);
				
				
				DBApp.serialize(toChange.nodeName, toChange);
				System.out.println("serialized in method UpdateNode(" + key + ") in Class Node:" + toChange + "\n");
				System.out.println("Node Update Success");
				return;
			}
		}
		System.out.println("UpdateFailed At Node: " + toChange + ", with Key: " + key);
	}

	public int getNodeFirstPage(K key, int pages) {

		RootNode r = (RootNode) DBApp.DeSerialize("data/" + tableName + IndexNum + "ROOT" + ".ser");
		Node n1 = (Node) DBApp.DeSerialize(r.Path);
		LeafNode n = (LeafNode) n1.getNode(key);
		K firstBiggestNum = null;
		boolean found = false;

		for (int i = 0; i < n.keys.size(); i++) {
			if (n.keys.get(i).compareTo(key) >= 0) {
				firstBiggestNum = n.keys.get(i);
				found = true;
				break;
			}
		}

		if (!found && n.next != null) {
			n = (LeafNode) DBApp.DeSerialize(n.next);
			firstBiggestNum = n.keys.get(0);
			found = true;
		}

		if (found) {

			Vector<int[]> vec = n1.getValue(firstBiggestNum);
			System.out.println(vec.size());
			int min = vec.firstElement()[0];
			for (int i = 1; i < vec.size(); i++) {
				if (vec.get(i)[0] < min)
					min = vec.get(i)[0];
			}
			return min;
		} else
			return pages;
	}

	/**
	 * Returns the value to which the specified key is associated, or {@code null}
	 * if this tree contains no association for the key.
	 *
	 * <p>
	 * A return value of {@code null} does not <i>necessarily</i> indicate that the
	 * tree contains no association for the key; it's also possible that the tree
	 * explicitly associates the key to {@code null}.
	 * 
	 *
	 */
	public Vector<int[]> search(K key) {
		RootNode r = (RootNode) DBApp.DeSerialize("data/" + tableName + IndexNum + "ROOT" + ".ser");
		Node n1 = (Node) DBApp.DeSerialize(r.Path);
		return n1.getValue(key);
	}

	/**
	 * Returns the values associated with the keys specified by the range:
	 * {@code key1} and {@code key2}.

	 *         {@code key1} and {@code key2}
	 */

	public List<Vector<int[]>> searchRange(K key1, RangePolicy policy1, K key2, RangePolicy policy2) {
		RootNode r = (RootNode) DBApp.DeSerialize("data/" + tableName + IndexNum + "ROOT" + ".ser");
		Node n1 = (Node) DBApp.DeSerialize(r.Path);
		return n1.getRange(key1, policy1, key2, policy2);
	}
	
	public List<Vector<int[]>> searchBiggerThan(K key1, RangePolicy policy1){
		RootNode r = (RootNode) DBApp.DeSerialize("data/" + tableName + IndexNum + "ROOT" + ".ser");
		Node n1 = (Node) DBApp.DeSerialize(r.Path);
		return n1.getBiggerThan(key1, policy1);
	}
	
	public List<Vector<int[]>> searchLessThan(K key1, RangePolicy policy1){
		RootNode r = (RootNode) DBApp.DeSerialize("data/" + tableName + IndexNum + "ROOT" + ".ser");
		Node n1 = (Node) DBApp.DeSerialize(r.Path);
		return n1.getLessThan(key1, policy1);
	}

	/**
	 * Associates the specified value with the specified key in this tree. If the
	 * tree previously contained a association for the key, the old value is
	 * replaced.
	 * 

	 */
	public void insert(K key, int page) {
		RootNode r = (RootNode) DBApp.DeSerialize("data/" + tableName + IndexNum + "ROOT" + ".ser");
		Node n1 = (Node) DBApp.DeSerialize(r.Path);
		n1.insertValue(key, page);
	}

	/**
	 * Removes the association for the specified key from this tree if present.
	 * 
	 */
	public void delete(K key, int page) {
		RootNode r = (RootNode) DBApp.DeSerialize("data/" + tableName + IndexNum + "ROOT" + ".ser");
		Node n1 = (Node) DBApp.DeSerialize(r.Path);
		n1.deleteValue(key, page);
	}

	public String toString() {
		Queue<List<Node>> queue = new LinkedList<List<Node>>();
		RootNode r = (RootNode) DBApp.DeSerialize("data/" + tableName + IndexNum + "ROOT" + ".ser");
		Node n1 = (Node) DBApp.DeSerialize(r.Path);
		queue.add(Arrays.asList(n1));
		StringBuilder sb = new StringBuilder();
		while (!queue.isEmpty()) {
			Queue<List<Node>> nextQueue = new LinkedList<List<Node>>();
			while (!queue.isEmpty()) {
				List<Node> nodes = queue.remove();
				sb.append('{');
				Iterator<Node> it = nodes.iterator();
				while (it.hasNext()) {
					Node node = it.next();
					sb.append(node.toString());
					for (int i = 0; i < node.keys.size(); i++) {

						for (int j = 0; j < node.getValue(node.keys.get(i)).size(); j++) {
							sb.append("(");
							sb.append(node.getValue(node.keys.get(i)).get(j)[0] + ","
									+ node.getValue(node.keys.get(i)).get(j)[1]);
							sb.append(")");
						}

						sb.append(" ");
					}
					if (it.hasNext())
						sb.append(", ");
					if (node instanceof BPlusTree.InternalNode) {
						List<Node> lchilds = new ArrayList<Node>();
						for (int i = 0; i < ((InternalNode) node).children.size(); i++) {
							lchilds.add((Node) DBApp.DeSerialize(((InternalNode) node).children.get(i)));
						}
						nextQueue.add(lchilds);
					}
				}
				sb.append('}');
				if (!queue.isEmpty())
					sb.append(", ");
				else
					sb.append('\n');
			}
			queue = nextQueue;
		}

		return sb.toString();
	}

	abstract class Node implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2589178481946008647L;
		List<K> keys;
		String nodeName;

		int keyNumber() {
			return keys.size();
		}

		abstract Vector<int[]> getValue(K key);

		// added
		abstract Node getNode(K key);

		abstract void deleteValue(K key, int page);

		abstract void insertValue(K key, int page);

		abstract K getFirstLeafKey();

		abstract List<Vector<int[]>> getRange(K key1, RangePolicy policy1, K key2, RangePolicy policy2);
		
		abstract List<Vector<int[]>> getBiggerThan(K key1, RangePolicy policy1);
		
		abstract List<Vector<int[]>> getLessThan(K key1, RangePolicy policy1);

		abstract void merge(Node sibling);

		abstract Node split();

		abstract boolean isOverflow();

		abstract boolean isUnderflow();

		public String toString() {
			return keys.toString();
		}
	}

	class InternalNode extends Node implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6353075387227431661L;
		List<String> children;

		InternalNode() {
			// numNodes++;
			String id = UUID.randomUUID().toString();
			nodeName = "data/" + tableName + IndexNum + "IndexNode" + id + ".ser";

			this.keys = new ArrayList<K>();
			this.children = new ArrayList<String>();
		}

		@Override
		Vector<int[]> getValue(K key) {
			return getChild(key).getValue(key);
		}

		// added
		Node getNode(K key) {
			return getChild(key).getNode(key);
		}

		@Override
		void deleteValue(K key, int page) {

			Node child = getChild(key);
			child.deleteValue(key, page);
			if (child.isUnderflow()) {
				Node childLeftSibling = getChildLeftSibling(key);
				Node childRightSibling = getChildRightSibling(key);

				Node left = childLeftSibling != null ? childLeftSibling : child;
				Node right = childLeftSibling != null ? child : childRightSibling;

				left.merge(right);

				// check
				deleteChild(right.getFirstLeafKey());

				if (left.isOverflow()) {
					Node sibling = left.split();
					insertChild(sibling.getFirstLeafKey(), sibling);
				}
				RootNode r = (RootNode) DBApp.DeSerialize("data/" + tableName + IndexNum + "ROOT" + ".ser");
				Node n1 = (Node) DBApp.DeSerialize(r.Path);
				if (n1.keyNumber() == 0)
					// root = left.nodeName;
					r.Path = left.nodeName;
				DBApp.serialize("data/" + tableName + IndexNum + "ROOT" + ".ser", r);
				System.out.println("serialized ----NEWROOT---- method deleteValue(" + key + ") in Class InternalNode:"
						+ left + "\n");
			}
		}

		@Override
		void insertValue(K key, int page) {
			Node child = getChild(key);
			child.insertValue(key, page);

			if (child.isOverflow()) {

				Node sibling = child.split();
				insertChild(sibling.getFirstLeafKey(), sibling);

			}
			RootNode r = (RootNode) DBApp.DeSerialize("data/" + tableName + IndexNum + "ROOT" + ".ser");
			Node n1 = (Node) DBApp.DeSerialize(r.Path);
			if (n1.isOverflow()) {
				Node sibling = split();
				InternalNode newRoot = new InternalNode();
				newRoot.keys.add(sibling.getFirstLeafKey());
				newRoot.children.add(this.nodeName);
				newRoot.children.add(sibling.nodeName);
				// root = newRoot.nodeName;

				r.Path = newRoot.nodeName;
				DBApp.serialize("data/" + tableName + IndexNum + "ROOT" + ".ser", r);
				System.out.println("serialized ----NEWROOT---- method insertValue(" + key + ") in Class InternalNode:"
						+ newRoot + "\n");

				DBApp.serialize(newRoot.nodeName, newRoot);
				System.out.println(
						"serialized in method insertValue(" + key + ") in Class InternalNode:" + newRoot + "\n");
			}
		}

		@Override
		K getFirstLeafKey() {
			Node child = (Node) DBApp.DeSerialize(children.get(0));
			System.out.println("Deserialized in method getFirstLeafKey() in Class InternalNode:" + child + "\n");
			return child.getFirstLeafKey();
		}

		@Override
		List<Vector<int[]>> getRange(K key1, RangePolicy policy1, K key2, RangePolicy policy2) {
			return getChild(key1).getRange(key1, policy1, key2, policy2);
		}
		
		List<Vector<int[]>> getBiggerThan(K key1, RangePolicy policy1){
			return getChild(key1).getBiggerThan(key1, policy1);
		}
		
		List<Vector<int[]>> getLessThan(K key1, RangePolicy policy1){
			Node child = (Node) DBApp.DeSerialize(children.get(0));
			return child.getLessThan(key1, policy1);
		}

		@Override
		void merge(Node sibling) {
			@SuppressWarnings("unchecked")
			InternalNode node = (InternalNode) sibling;
			keys.add(node.getFirstLeafKey());
			keys.addAll(node.keys);
			children.addAll(node.children);

			// DBApp.serialize(sibling.nodeName, sibling);
			// System.out.println("serialized in method merge() in Class
			// InternalNode:"+sibling+"\n");

			// check
			DBApp.serialize(this.nodeName, this);
			System.out.println("serialized in method merge() in Class InternalNode:" + this + "\n");

		}

		@Override
		Node split() {
			int from = keyNumber() / 2 + 1, to = keyNumber();
			InternalNode sibling = new InternalNode();

			sibling.keys.addAll(keys.subList(from, to));
			sibling.children.addAll(children.subList(from, to + 1));

			keys.subList(from - 1, to).clear();
			children.subList(from, to + 1).clear();

			DBApp.serialize(this.nodeName, this);
			System.out.println("serialized in method split() in Class InternalNode:" + this + "\n");

			DBApp.serialize(sibling.nodeName, sibling);
			System.out.println("serialized in method split() in Class InternalNode:" + sibling + "\n");

			return sibling;
		}

		@Override
		boolean isOverflow() {
			return children.size() > branchingFactor;
		}

		@Override
		boolean isUnderflow() {
			return children.size() < (branchingFactor + 1) / 2;
		}

		Node getChild(K key) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			Node child = (Node) DBApp.DeSerialize(children.get(childIndex));
			System.out.println("Deserialized in method getChild( " + key + " ) in Class InternalNode:" + child + "\n");
			return child;
		}

		void deleteChild(K key) {
			int loc = Collections.binarySearch(keys, key);
			if (loc >= 0) {
				String childToRemove = children.get(loc + 1);
				;
				keys.remove(loc);
				children.remove(loc + 1);

				DBApp.serialize(this.nodeName, this);
				System.out
						.println("serialized in method deleteChild(" + key + ") in Class InternalNode:" + this + "\n");
				// delete children.remove(loc + 1);?
				File file = new File(childToRemove);

				Node n = (Node) DBApp.DeSerialize(childToRemove);

				if (file.delete()) {
					System.out.println("File " + childToRemove + ":" + n + " deleted successfully");
				} else {
					System.out.println("Failed to delete the file " + childToRemove + ":" + n);
				}
			}
		}

		void insertChild(K key, Node child) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (loc >= 0) {
				children.set(childIndex, child.nodeName);
			} else {
				keys.add(childIndex, key);
				children.add(childIndex + 1, child.nodeName);
			}
			DBApp.serialize(this.nodeName, this);
			System.out.println("serialized in method insertChild(" + key + ") in Class InternalNode:" + this + "\n");

		}

		Node getChildLeftSibling(K key) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (childIndex > 0) {
				Node child = (Node) DBApp.DeSerialize(children.get(childIndex - 1));
				System.out.println("Deserialized in method getChildLeftSibling(" + key + ") in Class InternalNode:"
						+ child + "\n");
				return child;
			}

			return null;
		}

		Node getChildRightSibling(K key) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (childIndex < keyNumber()) {
				Node child = (Node) DBApp.DeSerialize(children.get(childIndex + 1));
				System.out.println("Deserialized in method getChildRightSibling(" + key + ") in Class InternalNode:"
						+ child + "\n");
				return child;
			}

			return null;
		}
	}

	// LEAF-NODE

	class LeafNode extends Node implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1701512332742793617L;
		List<Vector<int[]>> values;
		String next;

		LeafNode() {
			// numNodes++;
			String id = UUID.randomUUID().toString();
			nodeName = "data/" + tableName + IndexNum + "IndexNode" + id + ".ser";
			keys = new ArrayList<K>();
			values = new ArrayList<Vector<int[]>>();
		}

		@Override
		Vector<int[]> getValue(K key) {
			int loc = Collections.binarySearch(keys, key);
			return loc >= 0 ? values.get(loc) : null;
		}

		// added
		Node getNode(K key) {
			return this;
		}

		@Override
		void deleteValue(K key, int page) {
			boolean deleted = false;
			int loc = Collections.binarySearch(keys, key);
			if (loc >= 0) {
				Vector<int[]> vec = values.get(loc);
				for (int i = 0; i < vec.size(); i++) {
					int[] tupleAtLoc = vec.get(i);
					if (page == tupleAtLoc[0]) {
						tupleAtLoc[1]--;

						if (tupleAtLoc[1] == 0)
							vec.remove(i);

						if (vec.isEmpty()) {
							keys.remove(loc);
							values.remove(loc);
						}

						deleted = true;
						DBApp.serialize(this.nodeName, this);
						System.out.println(
								"serialized in method deleteValue(" + key + ") in Class LeafNode:" + this + "\n");

						break;
					}
				}
			}
			if (!deleted)
				System.out.println("--Deletion failed--");
		}

		@Override
		void insertValue(K key, int page) {
			int loc = Collections.binarySearch(keys, key);
			int valueIndex = loc >= 0 ? loc : -loc - 1;
			if (loc >= 0) {
				boolean found = false;
				Vector<int[]> vec = values.get(loc);
				for (int i = 0; i < vec.size(); i++) {
					int[] tupleAtLoc = vec.get(i);

					if (tupleAtLoc[0] == page) {
						tupleAtLoc[1]++;
						found = true;
						break;
					}
				}
				if (!found) {
					boolean added= false;
					int[] tuple = { page, 1 };
					for(int i=0 ; i<vec.size() ; i++) {
						if(vec.get(i)[0]>page) {
							vec.add(i, tuple);
							added=true;
							break;
						}
					}
					if(!added)vec.add(vec.size(),tuple);
				}
				// values.set(valueIndex, value);
				DBApp.serialize(this.nodeName, this);
				System.out.println("serialized in method insertValue(" + key + ") in Class LeafNode:" + this + "\n");

			} else {
				Vector<int[]> v = new Vector<int[]>();
				int[] element = { page, 1 };
				v.add(element);
				keys.add(valueIndex, key);
				values.add(valueIndex, v);

				DBApp.serialize(this.nodeName, this);
				System.out.println("serialized in method insertValue(" + key + ") in Class LeafNode:" + this + "\n");
			}

			RootNode r = (RootNode) DBApp.DeSerialize("data/" + tableName + IndexNum + "ROOT" + ".ser");
			Node n1 = (Node) DBApp.DeSerialize(r.Path);
			if (n1.isOverflow()) {
				Node sibling = split();
				InternalNode newRoot = new InternalNode();
				newRoot.keys.add(sibling.getFirstLeafKey());
				newRoot.children.add(this.nodeName);
				newRoot.children.add(sibling.nodeName);
				// root = newRoot.nodeName;

				r.Path = newRoot.nodeName;
				DBApp.serialize("data/" + tableName + IndexNum + "ROOT" + ".ser", r);
				System.out.println("serialized ----NEWROOT---- method insertValue(" + key + ") in Class LeafNode:"
						+ newRoot + "\n");

				DBApp.serialize(newRoot.nodeName, newRoot);
				System.out.println("serialized in method insertValue(" + key + ") in Class LeafNode:" + newRoot + "\n");
			}
		}

		@Override
		K getFirstLeafKey() {
			return keys.get(0);
		}

		@Override
		List<Vector<int[]>> getRange(K key1, RangePolicy policy1, K key2, RangePolicy policy2) {
			List<Vector<int[]>> result = new LinkedList<Vector<int[]>>();
			LeafNode node = this;
			while (node != null) {
				Iterator<K> kIt = node.keys.iterator();
				Iterator<Vector<int[]>> vIt = node.values.iterator();
				while (kIt.hasNext()) {
					K key = kIt.next();
					Vector<int[]> value = vIt.next();
					int cmp1 = key.compareTo(key1);
					int cmp2 = key.compareTo(key2);
					if (((policy1 == RangePolicy.EXCLUSIVE && cmp1 > 0)
							|| (policy1 == RangePolicy.INCLUSIVE && cmp1 >= 0))
							&& ((policy2 == RangePolicy.EXCLUSIVE && cmp2 < 0)
									|| (policy2 == RangePolicy.INCLUSIVE && cmp2 <= 0)))
						result.add(value);
					else if ((policy2 == RangePolicy.EXCLUSIVE && cmp2 >= 0)
							|| (policy2 == RangePolicy.INCLUSIVE && cmp2 > 0))
						return result;
				}
				if(node.next!=null)
				node = (LeafNode) DBApp.DeSerialize(node.next);
				else break;
			}
			return result;
		}
		
		List<Vector<int[]>> getBiggerThan(K key1, RangePolicy policy1) {
			List<Vector<int[]>> result = new LinkedList<Vector<int[]>>();
			LeafNode node = this;
			boolean PassedFirstPage=false;
			while (node != null) {
				
				Iterator<K> kIt = node.keys.iterator();
				Iterator<Vector<int[]>> vIt = node.values.iterator();
				while (kIt.hasNext()) {
					K key = kIt.next();
					Vector<int[]> value = vIt.next();
					int cmp1 = key.compareTo(key1);
					if (((policy1 == RangePolicy.EXCLUSIVE && cmp1 > 0)
							|| (policy1 == RangePolicy.INCLUSIVE && cmp1 >= 0)))		
						result.add(value);
					else if(PassedFirstPage) return result;
				}
				if(node.next!=null) {
					node = (LeafNode) DBApp.DeSerialize(node.next);
					PassedFirstPage=true;
				}
				else break;
			}
			return result;
		}
		
		List<Vector<int[]>> getLessThan(K key1, RangePolicy policy1){
			List<Vector<int[]>> result = new LinkedList<Vector<int[]>>();
			LeafNode node = this;
			boolean PassedFirstPage=false;
			while (node != null) {
				
				Iterator<K> kIt = node.keys.iterator();
				Iterator<Vector<int[]>> vIt = node.values.iterator();
				while (kIt.hasNext()) {
					K key = kIt.next();
					Vector<int[]> value = vIt.next();
					int cmp1 = key.compareTo(key1);
					if (((policy1 == RangePolicy.EXCLUSIVE && cmp1 < 0)
							|| (policy1 == RangePolicy.INCLUSIVE && cmp1 <= 0)))		
						result.add(value);
					else if(PassedFirstPage) return result;
				}
				if(node.next!=null) {
					node = (LeafNode) DBApp.DeSerialize(node.next);
					PassedFirstPage=true;
				}
				else break;
			}
			return result;
		}

		@Override
		void merge(Node sibling) {
			@SuppressWarnings("unchecked")
			LeafNode node = (LeafNode) sibling;
			keys.addAll(node.keys);
			values.addAll(node.values);
			next = node.next;

			// DBApp.serialize(sibling.nodeName, sibling);
			// System.out.println("serialized in method merge() in Class
			// LeafNode:"+sibling+"\n");

			// check
			DBApp.serialize(this.nodeName, this);
			System.out.println("serialized in method merge() in Class LeafNode:" + this + "\n");
		}

		@Override
		Node split() {
			LeafNode sibling = new LeafNode();
			int from = (keyNumber() + 1) / 2, to = keyNumber();
			sibling.keys.addAll(keys.subList(from, to));
			sibling.values.addAll(values.subList(from, to));

			keys.subList(from, to).clear();
			values.subList(from, to).clear();

			sibling.next = next;
			next = sibling.nodeName;

			DBApp.serialize(sibling.nodeName, sibling);
			System.out.println("serialized in method split() in Class LeafNode:" + sibling + "\n");

			DBApp.serialize(this.nodeName, this);
			System.out.println("serialized in method split() in Class LeafNode:" + this + "\n");

			return sibling;
		}

		@Override
		boolean isOverflow() {
			return values.size() > branchingFactor - 1;
		}

		@Override
		boolean isUnderflow() {
			return values.size() < branchingFactor / 2;
		}
	}

	/*
	 * public static void main(String[] args) {
	 * 
	 * //BPlusTree<Integer, String> bpt = new BPlusTree<Integer, String>(4,
	 * "Student", "id", 1); Random random = new Random(); int[] n = new int[50];
	 * 
	 * for (int i = 0; i < 0; i++) { n[i] = random.nextInt(50); }
	 * 
	 * for (int i = 0; i < 0; i++) { //bpt.insert(n[i], i); }
	 * 
	 * System.out.print("{"); for (int i = 0; i < 0; i++) { System.out.print(n[i] +
	 * ","); } System.out.println("}");
	 * 
	 * //DBApp.serialize("bpt.ser", bpt);
	 * 
	 * BPlusTree bpt = (BPlusTree) DBApp.DeSerialize("bpt.ser");
	 * System.out.println(bpt);
	 * 
	 * }
	 */

}