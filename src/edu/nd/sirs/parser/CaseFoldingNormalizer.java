package edu.nd.sirs.parser;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Normalizes text by case folding
 * 
 * @author tweninge
 *
 */
public class CaseFoldingNormalizer implements INormalizer {

	private static Logger logger = LoggerFactory.getLogger(CaseFoldingNormalizer.class);

	/**
	 * HW2, you'll need to change this code.
	 */
	public List<String> normalize(List<String> str) {
		logger.info("Case Folding Normalizer...");
		List<String> newS = new ArrayList<String>();
		for (String s : str){
			newS.add(s.toLowerCase());
		}
		return newS;
	}
}
