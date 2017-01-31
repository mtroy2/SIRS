package edu.nd.sirs.index;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.PriorityQueue;

/**
 * RunFile class keep information about the current position of a cursor in a
 * run file.
 * 
 * @author tweninge
 *
 */
public class RunFile {
	File filename;
	int buffersize;
	long currentPos;
	long length;

	PriorityQueue<DocumentTerm> buffer;

	/**
	 * Constructor
	 * 
	 * @param file
	 *            Run file
	 * @param bsize
	 *            buffer size to fill
	 */
	public RunFile(File file, int bsize) {
		filename = file;
		buffersize = bsize;
		currentPos = 0;
		getFileSize();
		buffer = new PriorityQueue<DocumentTerm>(buffersize);
	}

	/**
	 * Initialize the size of the file.
	 */
	private void getFileSize() {
		length = filename.length();
	}

	/**
	 * Read and parse postings within a single line for until buffer is full.
	 * 
	 * @return True if postings were read, false otherwise.
	 * @throws IOException
	 */
	private boolean fillBuffer() throws IOException {
		boolean readsome = false;
		RandomAccessFile raf = new RandomAccessFile(filename, "r");
		raf.seek(currentPos);
		DocumentTerm p;
		int bufsize = buffer.size();

		while ((currentPos < length) && (bufsize < buffersize)) {
			readsome = true;
			StringBuffer sb = new StringBuffer();
			char c = '-';
			while ((c = (char) raf.read()) != '\t') {
				sb.append(c);
			}
			int d = Integer.parseInt(sb.toString());
			sb = new StringBuffer();
			while ((c = (char) raf.read()) != '\t') {
				sb.append(c);
			}
			long t = Long.parseLong(sb.toString());
			sb = new StringBuffer();
			while ((c = (char) raf.read()) != '\n') {
				sb.append(c);
			}
			int f = Integer.parseInt(sb.toString().trim());

			p = new DocumentTerm(t, d, f);
			buffer.add(p);

			currentPos = raf.getFilePointer();
			++bufsize;
		}
		raf.close();
		return readsome;
	}

	/**
	 * Return a single posting from this run file.
	 * 
	 * @return A Posting
	 */
	public DocumentTerm getRecord() {
		if (buffer.size() > 0) {
			return buffer.poll();
		} else {
			try {
				if (fillBuffer()) {
					return buffer.poll();
				} else {
					return null;
				}
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
	}

}
