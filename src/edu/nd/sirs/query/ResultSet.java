package edu.nd.sirs.query;

import java.util.Collection;

import edu.nd.sirs.util.HeapSort;

/**
 * Result object that is passed to the Web search engine. Contains ranked list
 * of results and scores
 * 
 * @author tweninge
 *
 */
public class ResultSet {

	private int resultSize;
	private int exactResultSize;
	private int[] docids;
	private float[] scores;
	private short[] occurrences;

	/**
	 * Simple Constructor
	 * 
	 * @param q
	 *            List of hits
	 */
	public ResultSet(Collection<Hit> q) {
		resultSize = q.size();
		exactResultSize = resultSize;

		docids = new int[resultSize];
		scores = new float[resultSize];
		occurrences = new short[resultSize];

		int i = 0;
		for (Hit cc : q) {
			docids[i] = cc.getDocId();
			scores[i] = cc.getScore();
			occurrences[i] = cc.getOccurrence();
			i++;
		}
	}

	public float[] getScores() {
		return scores;
	}

	public void setExactResultSize(int numRetrievedDocs) {
		exactResultSize = numRetrievedDocs;
	}

	public void setResultSize(int setSize) {
		resultSize = setSize;
	}

	public int[] getDocids() {
		return docids;
	}

	public short[] getOccurrences() {
		return occurrences;
	}

	public int getResultSize() {
		return resultSize;
	}

	public int getExactResultSize() {
		return exactResultSize;
	}

	/**
	 * Sort documents such that we only return the top documents
	 * 
	 * @param topDocs
	 *            number of documents to return after sorting
	 */
	public void sort(int topDocs) {
		HeapSort.descendingHeapSort(getScores(), getDocids(), getOccurrences(),
				topDocs);
	}

}
