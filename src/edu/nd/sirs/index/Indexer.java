package edu.nd.sirs.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.nd.sirs.docs.Document;
import edu.nd.sirs.docs.Field;
import edu.nd.sirs.docs.Fields;
import edu.nd.sirs.docs.HTMLDocument;
import edu.nd.sirs.docs.Token;

/**
 * Creates direct and inverted indexes for the documents stored in the folder.
 * 
 * @author tweninge
 *
 */
public class Indexer {

	private static Logger logger = LoggerFactory.getLogger(Indexer.class);

	private static final String DOCIDX = "./data/doc_idx.txt";
	private static final String DOCIDXOFFSET = "./data/doc_idx_offset.txt";
	private static final String LEXICON = "./data/lex.txt";
	private static final String RUNSPREFIX = "./data/runs/run";
	private static final String IDX = "./data/idx.txt";
	private static final String IDXTERMOFFSET = "./data/idx_term_offset.txt";
	private static final String ANCIDX = "./data/anc_idx.txt";

	private static final Integer RUN_SIZE = 100000;
	private static final Boolean COMPRESS = false;

	private int wordId;
	private int docId;
	private List<DocumentTerm> run;
	private int runNumber;

	private TreeMap<String, Integer> voc;
	private TreeMap<String, Integer> docs;

	/**
	 * Indexer Constructor
	 */
	public Indexer() {
		wordId = 0;
		docId = 0;
		runNumber = 0;
		voc = new TreeMap<String, Integer>();
		docs = new TreeMap<String, Integer>();
	}

	/**
	 * Create direct and inverted indices for each file in the list of files.
	 * 
	 * @param filesToIndex
	 *            files to index
	 */
	private void indexDirectory(File crawlFile) {
		docId = 0;

		PrintWriter docWriter;
		PrintWriter ancWriter;
		PrintWriter docWriterOffset;
		try {
			docWriter = new PrintWriter(DOCIDX, "UTF-8");
			docWriterOffset = new PrintWriter(DOCIDXOFFSET, "UTF-8");

			ancWriter = new PrintWriter(ANCIDX, "UTF-8");

			// start the first run
			logger.info("Starting the first indexer run.");
			run = new ArrayList<DocumentTerm>();
			int written = 0;
			
			ZipFile zip = new ZipFile(crawlFile);
			Enumeration<? extends ZipEntry> enties = zip.entries();

			while (enties.hasMoreElements()) {
				ZipEntry file = enties.nextElement();
			
				logger.info("Indexing document " + file.getName());				
				Document doc = new HTMLDocument(docId, file);
				List<Token> tokens = doc.parse(docId, zip.getInputStream(file));
				
				List<String> toRemove = new ArrayList<String>();

				StringBuffer sb = new StringBuffer();
				sb.append(doc.getDocId());
				for (Entry<String, Object> e : doc.getResources().entrySet()) {
					if (e.getKey().startsWith("l")) {
						sb.append("\t"
								+ URLEncoder.encode(e.getKey().substring(1),
										"UTF-8"));
						for (Token t : (List<Token>) e.getValue()) {
							if (t.getTokenString().trim().isEmpty()) {
								continue;
							}
							sb.append(":" + t.getTokenString() + ","
									+ t.getField().field);
						}
						toRemove.add(e.getKey());
					}
				}
				ancWriter.print(sb.toString() + "\n");

				for (String r : toRemove) {
					doc.getResources().remove(r);
				}

				index(tokens);
				String s = doc.getName();
				if (s.endsWith("%2F")) {
					s = s.substring(0, s.lastIndexOf("%2F"));
				}
				docs.put(s, docId);
				docWriterOffset.write(written + "\n");

				// Writing to Direct Index
				String idxable = doc.writeToIndex();
				docWriter.write(idxable);				
				written += StringUtils.getBytesUtf8(idxable).length;
				docId++;
			}
			docWriter.close();
			ancWriter.close();
			docWriterOffset.close();
			zip.close();

			indexIncomingAnchorText();

			// If there is something yet in the last run, sort it and store
			if (run.size() > 0) {
				logger.info("Writing file run to disk.");
				storeRun();
			}

			logger.info("Indexing runs complete.");
		} catch (FileNotFoundException e) {
			logger.error("Cannot find direct index file.", e);
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {			
			e1.printStackTrace();
		}

		try {
			mergeRuns();
		} catch (FileNotFoundException e) {
			logger.error("Cannot find inverted index file.", e);
		}

		// Output the vocabulary
		try {
			outputLexicon();
		} catch (FileNotFoundException e) {
			logger.error("Cannot find lexicon file.", e);
		}
		logger.info("Indexing complete.");
	}

	private void indexIncomingAnchorText() throws FileNotFoundException {
		BufferedReader br = new BufferedReader(new FileReader(new File(ANCIDX)));
		String line = "";
		Map<Integer, Integer> docIDlength = new HashMap<Integer, Integer>();
		try {
			while ((line = br.readLine()) != null) {
				String[] a = line.split("\t");
				int doci = Integer.parseInt(a[0]);
				for (int i = 1; i < a.length; i++) {
					String[] b = a[i].split(":");
					String url = b[0];
					if (url.endsWith("%2F")) {
						url = url.substring(0, url.lastIndexOf("%2F"));
					}
					List<Token> toks = new ArrayList<Token>();
					for (int j = 1; j < b.length; j++) {
						String[] c = b[j].split(",");
						String s = c[0];
						if (s.isEmpty())
							continue;
						Field field = new Field(Integer.parseInt(c[1]));
						toks.add(new Token(s, field));
					}
					if (docs.containsKey(url)) {
						index(toks, docs.get(url));
						if (!docIDlength.containsKey(docs.get(url))) {
							docIDlength.put(docs.get(url), toks.size());
						} else {
							docIDlength.put(
									docs.get(url),
									docIDlength.get(docs.get(url))
											+ toks.size());
						}
					}
				}
			}
			br.close();
			reindexDocuments(docIDlength);

		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void reindexDocuments(Map<Integer, Integer> docIDlength)
			throws FileNotFoundException {

		// BufferedReader br = new BufferedReader(new FileReader(new
		// File(DOCIDX)));

		PrintWriter docWriter = null;
		try {
			docWriter = new PrintWriter(DOCIDX + "n", "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}
		PrintWriter docWriterOffset = null;
		try {
			docWriterOffset = new PrintWriter(DOCIDXOFFSET + "n", "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}
		long offset = 0;
		String line = "";
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new FileInputStream(DOCIDX), "UTF-8"));

			while ((line = br.readLine()) != null) {
				String[] l = line.split("\t");
				Integer dID = Integer.parseInt(l[0]);
				String len = l[2];
				if (docIDlength.containsKey(dID)) {
					len = len + "," + Fields.getInstance().getFieldId("link")
							+ ":" + docIDlength.get(dID);
				}
				StringBuffer sb = new StringBuffer();
				sb.append(l[0]);
				for (int i = 1; i < l.length; i++) {
					if (i == 2) {
						sb.append("\t").append(len);
					} else {
						sb.append("\t").append(l[i]);
					}
				}
				docWriter.print(sb.append("\n").toString());
				docWriterOffset.println(offset);
				offset += StringUtils.getBytesUtf8(sb.toString()).length;
			}
			br.close();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		docWriter.close();
		docWriterOffset.close();

		File f = new File(DOCIDX);
		f.delete();
		new File(DOCIDXOFFSET).delete();
		new File(DOCIDX + "n").renameTo(new File(DOCIDX));
		new File(DOCIDXOFFSET + "n").renameTo(new File(DOCIDXOFFSET));
	}

	private void outputLexicon() throws FileNotFoundException {
		logger.info("Writing lexicon to disk");
		PrintWriter lexFile = new PrintWriter(LEXICON);
		for (Entry<String, Integer> x : voc.entrySet()) {
			lexFile.println(x.getKey() + "\t" + x.getValue());
		}
		lexFile.close();
		logger.info("Lexicon writing finished");
	}

	/**
	 * Merge the runs together to make a single inverted index
	 * 
	 * @throws FileNotFoundException
	 */
	private void mergeRuns() throws FileNotFoundException {

		// Create the heap
		PriorityQueue<MergeDocumentTerms> mergeHeap = new PriorityQueue<MergeDocumentTerms>();
		List<RunFile> rfv = new ArrayList<RunFile>();
		String filename;
		DocumentTerm ocurr;
		MergeDocumentTerms ro;
		for (int i = 0; i < runNumber; ++i) {
			filename = RUNSPREFIX + i;
			rfv.add(new RunFile(new File(filename), RUN_SIZE / runNumber));
			// get the first element and put it in the heap
			ocurr = rfv.get(i).getRecord();
			if (ocurr == null) {
				logger.error("Error: Record was not found.");
				return;
			}
			ro = new MergeDocumentTerms(ocurr, i);
			mergeHeap.add(ro);
		}

		PrintWriter outFile = null;
		try {
			outFile = new PrintWriter(IDX, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		// encode the fields in the invertedIndex
		StringBuffer sb = new StringBuffer();
		for (Entry<String, Field> f : Fields.getInstance().getEntries()) {
			sb.append(f.getKey() + "," + f.getValue().field + ";");
		}
		outFile.print(sb.toString() + "\n");

		long currentTerm = 0l;		
		long currentTermOffset = StringUtils.getBytesUtf8(sb.toString()).length+ 1;

		PrintWriter tosFile = null;
		try {
			tosFile = new PrintWriter(IDXTERMOFFSET, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		String wid = wordId + "\n";
		tosFile.print(wid);

		MergeDocumentTerms first;
		logger.info("Merging run files...");

		int df = 0;
		TreeMap<Field, StringBuffer> posting = new TreeMap<Field, StringBuffer>();
		for (Field f : Fields.getInstance().getFields()) {
			posting.put(f, new StringBuffer());
		}

		while (!mergeHeap.isEmpty()) {
			first = mergeHeap.poll();

			// Get a new posting from the same run and
			// put it in the heap, if possible
			ocurr = rfv.get(first.run).getRecord();
			if (ocurr != null) {
				ro = new MergeDocumentTerms(ocurr, first.run);
				mergeHeap.add(ro);
			}
			// Saving to the file
			if (first.getTermId() > currentTerm) {
				tosFile.print(currentTermOffset + "\n");

				sb = new StringBuffer();
				for (Field f : Fields.getInstance().getFields()) {
					sb.append("#" + f.field + posting.get(f));
				}

				String p = currentTerm + ":" + df + "\t" + sb.toString() + "\n";
				outFile.print(p);				
				currentTermOffset += StringUtils.getBytesUtf8(p).length;
				currentTerm = first.getTermId();
				for (Field f : Fields.getInstance().getFields()) {
					posting.put(f, new StringBuffer());
				}
				df = 0;
			} else if (first.getTermId() < currentTerm) {
				logger.error("Term ids messed up, something went wrong with the sorting");
			}
			if (COMPRESS) {
				// not yet
			} else {
				df++;
				StringBuffer zsb = posting.get(first.getField());
				zsb.append("(" + first.getDocId() + "," + first.getFrequency()
						+ ");");
			}
		}
		outFile.close();
		tosFile.close();
		logger.info("Index merging finished");
	}

	/**
	 * Creates a local vocabulary and indexes terms one-by-one
	 * 
	 * @param tokens
	 *            list of tokens for indexing
	 */
	private void index(List<Token> tokens, int docId) {
		HashMap<Integer, DocumentTerm> lVoc = new HashMap<Integer, DocumentTerm>();
		for (Token token : tokens) {
			index(token, docId, lVoc);
		}

		for (DocumentTerm p : lVoc.values()) {
			if (run.size() < RUN_SIZE) {
				run.add(p);
			} else {
				logger.info("Current indexing run full, storing to disk.");
				storeRun();
				run.add(p);
			}
		}
	}

	/**
	 * Creates a local vocabulary and indexes terms one-by-one
	 * 
	 * @param tokens
	 *            list of tokens for indexing
	 */
	private void index(List<Token> tokens) {
		HashMap<Integer, DocumentTerm> lVoc = new HashMap<Integer, DocumentTerm>();
		for (Token token : tokens) {
			index(token, docId, lVoc);
		}

		for (DocumentTerm p : lVoc.values()) {
			if (run.size() < RUN_SIZE) {
				run.add(p);
			} else {
				logger.info("Current indexing run full, storing to disk.");
				storeRun();
				run.add(p);
			}
		}
	}

	/**
	 * Store the current run on disk.
	 */
	private void storeRun() {
		// creating the output file
		try {
			long runId = getRunNumber();
			File outName = new File(RUNSPREFIX + runId);
			if (!outName.getParentFile().exists()) {
				logger.info("Creating run directory");
				outName.getParentFile().mkdir();
			}
			if (outName.exists()) {
				logger.warn("Run directory already exists - deleting");
				outName.delete();
			}
			PrintWriter outFile = new PrintWriter(outName);

			logger.info("Sorting the current run");
			Collections.sort(run);

			// Storing it
			for (DocumentTerm p : run) {
				outFile.println(p.getDocId() + "\t" + p.getTermId() + "\t"
						+ p.getField().field + "\t" + p.getFrequency());
			}
			outFile.close();
		} catch (FileNotFoundException e) {
			logger.error("Cannot find run file within " + RUNSPREFIX, e);
		}
		run.clear();
	}

	/**
	 * Does needed math to return appropriate run number
	 * 
	 * @return current run number
	 */
	private long getRunNumber() {
		++runNumber;
		return runNumber - 1;
	}

	private int getNewId() {
		++wordId;
		return wordId - 1;
	}

	/**
	 * Creates a DocumentTerm pair from token and docid and adds it to the local
	 * vocabulary
	 * 
	 * @param token
	 *            Token to index
	 * @param docId
	 *            Document Id containing Token
	 * @param lVoc
	 *            local dictionary of Tokens->DocumentTerm
	 */
	private void index(Token token, int docId,
			HashMap<Integer, DocumentTerm> lVoc) {
		int termId;
		if (!voc.containsKey(token.getTokenString())) {
			termId = getNewId();
			voc.put(token.getTokenString(), termId);
		} else {
			termId = voc.get(token.getTokenString());
		}

		if (!lVoc.containsKey(termId)) {
			DocumentTerm p = new DocumentTerm(termId, docId, 1,
					token.getField());
			lVoc.put(termId, p);
		} else {
			DocumentTerm p = lVoc.get(termId);
			p.incrementFrequency();
			// do we need this?
			lVoc.put(termId, p);
		}
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

	private static final String CRL = "./data/crawl.zip";

	public static void main(String[] args) {
		File crawl = null;
		if (args.length == 1) {
			logger.info("Using user provided parameters");
			try {
				crawl = new File(args[0]);
			} catch (Exception e) {
				printUsage(e);
			}
		} else {
			logger.info("User did not provide 1 input argument; reverting to defaults...");
			crawl = new File(CRL);
		}

		Indexer idxr = new Indexer();		
		idxr.indexDirectory(crawl);
	}

	private static void printUsage(Exception e) {
		logger.error("Error parsing user provided parameters: "
				+ "Indexer <crawlerDataFolder>", e);
	}

}
