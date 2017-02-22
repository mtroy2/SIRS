package edu.nd.sirs.eval;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.nd.sirs.index.Indexer;
import edu.nd.sirs.query.ResultSet;

public class Evaluate {
	private static final String QRELS_FLDR = "./qrels/";
	private static final Integer REL_THRESH = 2;

	private static Logger logger = LoggerFactory.getLogger(Evaluate.class);

	private HashMap<String, Map<Integer, Integer>> rels;

	private float sumPrec = 0f;
	private float sumRecall = 0f;
	private float sumF1 = 0f;
	private float sumAvgPrec = 0f;
	private float sumMRR = 0f;
	private float sumNDCG = 0f;

	private int missing;

	public static void main(String[] args) {
		File qrels = null;
		if (args.length == 1) {
			logger.info("Using user provided parameters");
			try {
				qrels = new File(args[0]);
			} catch (Exception e) {
				printUsage(e);
			}
		} else {
			logger.info("User did not provide 1 input argument; reverting to defaults...");
			qrels = new File(QRELS_FLDR);
		}

		Evaluate g = new Evaluate();
		g.selfassess(g.getFiles(qrels));

	}

	public Evaluate() {
		this(new File(QRELS_FLDR));
	}

	public Evaluate(File qrels) {
		rels = new HashMap<String, Map<Integer, Integer>>();

		File[] files = getFiles(qrels);
		parse(files);
	}

	private void selfassess(File[] qrels) {

		try {
			for (File qrel : qrels) {
				sumPrec = 0f;
				sumRecall = 0f;
				sumF1 = 0f;
				sumAvgPrec = 0f;
				sumMRR = 0f;
				sumNDCG = 0f;

				List<Integer> tbl = null;
				BufferedReader br = new BufferedReader(new FileReader(qrel));
				String line;
				String q = "";
				float queries = 0;

				while ((line = br.readLine()) != null) {
					if (line.startsWith("query:")) {
						if (tbl != null) {
							run(tbl, q);
						}
						q = line.substring(7).trim();
						tbl = new ArrayList<Integer>();
						queries++;
					} else {
						// 1 4103 2
						String[] rel = line.split(" ");
						if (rel.length != 2) {
							logger.error("invalid qrels file: "
									+ qrel.getAbsolutePath());
							br.close();
							return;
						}
						tbl.add(Integer.parseInt(rel[0]));
					}
				}

				if (tbl != null) {
					run(tbl, q);
				}

				br.close();
				System.out
						.println("Evaluation Results for System codified in : "
								+ qrel.getName());
				System.out.println("Mean Precision: " + sumPrec / queries);
				System.out.println("Mean Recall: " + sumRecall / queries);
				System.out.println("Mean F1: " + sumF1 / queries);
				System.out.println("Mean Average Precision: " + sumAvgPrec
						/ queries);
				System.out.println("Mean Reciporal Rank: " + sumMRR / queries);
				System.out.println("Mean nDCG: " + sumNDCG / queries);
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public EvaluationResults evaluate(ResultSet rs, String q) {
		return this.evaluate(rs, q, rs.getResultSize());
	}

	public EvaluationResults evaluate(ResultSet rs, String q, int k) {
		List<Integer> tbl = new ArrayList<Integer>(k);
		for (int i = 0; i < k && i < rs.getResultSize(); i++) {
			tbl.add(rs.getDocids()[i]);
		}
		return run(tbl, q);
	}

	private EvaluationResults run(List<Integer> tbl, String q) {

		float precision = calcPrecision(tbl, q);
		sumPrec += precision;
		float recall = calcRecall(tbl, q);
		sumRecall += recall;
		float f1 = calcF(precision, recall, 1);
		sumF1 += f1;
		float avgPrec = calcAvgPrec(tbl, q);
		sumAvgPrec += avgPrec;
		float mrr = calcMRR(tbl, q);
		sumMRR += mrr;
		float ndcg = calcNDCG(tbl, q);
		sumNDCG += ndcg;

		return new EvaluationResults(missing, precision, recall, f1, avgPrec,
				mrr, ndcg);

	}

	private float calcNDCG(List<Integer> tbl, String q) {
		if (!rels.containsKey(q)) {
			logger.error("No relevance judgements for query: " + q);
			return 0;
		}

		float dcg = 0;
		float idcg = 0;
		for (int i = 0; i < tbl.size(); i++) {
			int docid = tbl.get(i);
			Integer rel = rels.get(q).get(docid);
			if (rel == null) {
				logger.warn("No relevance information for docID: " + docid);
				continue;
			}

			float num = (float) Math.pow(2.0, rel) - 1f;
			float den = (float) ((float) Math.log((float) (i + 1) + 1f) / Math
					.log(2d));
			dcg += num / den;
		}

		Map<Integer, Integer> ideal = null;

		ValueComparator bvc = new ValueComparator(rels.get(q));
		ideal = new TreeMap<Integer, Integer>(bvc);
		ideal.putAll(rels.get(q));

		int i = 1;
		for (Entry<Integer, Integer> e : ideal.entrySet()) {
			if (i > tbl.size())
				break; // we only go as high as tbl.size
			Integer rel = e.getValue();
			if (rel == null) {
				logger.warn("No relevance information for docID: " + e.getKey());
				continue;
			}

			float num = (float) Math.pow(2.0, rel) - 1f;
			float den = (float) ((float) Math.log((float) i + 1f) / Math
					.log(2d));
			idcg += num / den;
			i++;
		}
		return dcg / idcg;
	}

	class ValueComparator implements Comparator<Integer> {

		Map<Integer, Integer> base;

		public ValueComparator(Map<Integer, Integer> base) {
			this.base = base;
		}

		// Note: this comparator imposes orderings that are inconsistent with
		// equals.
		public int compare(Integer a, Integer b) {
			if (base.get(a) >= base.get(b)) {
				return -1;
			} else {
				return 1;
			} // returning 0 would merge keys
		}
	}

	private float calcMRR(List<Integer> tbl, String q) {
		if (!rels.containsKey(q)) {
			logger.error("No relevance judgements for query: " + q);
			return 0;
		}

		//TODO: Add code to calculate the mean reciprocal rank
		return 0;
	}

	private float calcAvgPrec(List<Integer> tbl, String q) {
		if (!rels.containsKey(q)) {
			logger.error("No relevance judgements for query: " + q);
			return 0;
		}

		//TODO: Add code to calculate the average precisions. Super hint: use the precision function!
		return 0;
	}

	private float calcF(float precision, float recall, int beta) {
		if ((precision + recall) == 0)
			return 0;
		//TODO: Add code to calculate the F_\beta measure
		return 0;
	}

	private float calcRecall(List<Integer> tbl, String q) {
		float tp = 0;
		float fn = 0;

		int totalRelevant = 0;
		if (!rels.containsKey(q)) {
			logger.error("No relevance judgements for query: " + q);
			return 0;
		}
		for (Integer rel : rels.get(q).values()) {
			if (rel > REL_THRESH) {
				totalRelevant++;
			}
		}

		for (Integer docid : tbl) {
			Integer rel = rels.get(q).get(docid);
			if (rel == null) {
				logger.warn("No relevance information for docID: " + docid);
				continue;
			}
			if (rel > REL_THRESH) {
				tp++;
			}
		}
		fn = totalRelevant - tp;
		if ((tp + fn) == 0)
			return 0;
		return tp / (tp + fn);
	}

	private float calcPrecision(List<Integer> tbl, String q) {
		return calcPrecision(tbl, q, Integer.MAX_VALUE);
	}

	private float calcPrecision(List<Integer> tbl, String q, int k) {
		float tp = 0;
		float fp = 0;
		if (!rels.containsKey(q)) {
			logger.error("No relevance judgements for query: " + q);
			missing = -1;
			return 0;
		}
		for (int i = 0; i <= k && i < tbl.size(); i++) {
			int docid = tbl.get(i);
			Integer rel = rels.get(q).get(docid);
			if (rel == null) {
				logger.warn("No relevance information for docID: " + docid);
				missing++;
				continue;
			}
			if (rel > REL_THRESH) {
				tp++;
			} else {
				fp++;
			}

		}
		return tp / (tp + fp);
	}

	/**
	 * 
	 * @param qrels
	 */
	private void parse(File[] qrels) {
		try {
			for (File qrel : qrels) {
				BufferedReader br = new BufferedReader(new FileReader(qrel));
				String line;
				String q = "";
				while ((line = br.readLine()) != null) {
					if (line.startsWith("query:")) {

						q = line.substring(7).trim();

						if (!rels.containsKey(q)) {
							rels.put(q, new HashMap<Integer, Integer>());
						}

					} else {
						// 1 4103 2
						String[] rel = line.split(" ");
						if (rel.length != 2) {
							logger.error("invalid qrels file: "
									+ qrel.getAbsolutePath());
							br.close();
							return;
						}
						rels.get(q).put(Integer.parseInt(rel[0]),
								Integer.parseInt(rel[1]));
					}
				}
				br.close();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void printUsage(Exception e) {
		logger.error("Error parsing user provided parameters: "
				+ "Gauntlet <qrels folder>", e);
	}

	/**
	 * Get files, and only files, from within the specified directory.
	 * 
	 * @param dir
	 *            directory in which to look for files
	 * @return array of files found in dir.
	 */
	private File[] getFiles(File dir) {
		if (!dir.isDirectory()) {
			logger.error(dir + " not a directory of files.");
			System.exit(1);
		}
		return dir.listFiles(new FilenameFilter() {
			/**
			 * Only accept files within the directory... do not recur into
			 * subdirectories.
			 */
			public boolean accept(File dir, String name) {
				return new File(dir, name).isFile();
			}

		});
	}

}
