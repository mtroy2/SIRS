package edu.nd.sirs.parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tokenizes based on non-word-character spaces (space, tab, comma, etc)
 * 
 * @author tweninge
 *
 */
public class WhitespaceTextTokenizer implements ITokenizer {

	public List<String> tokenize(FileReader fr) {
		BufferedReader br = new BufferedReader(fr);
		String line = "";
		ArrayList<String> tokens = new ArrayList<String>();
		try {
			while ((line = br.readLine()) != null) {
				for (String t : line.split("\\W+")) {
					tokens.add(t);
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return tokens;
	}

	public List<String> tokenize(String str) {
		return Arrays.asList(str.split("\\W+"));
	}
}
