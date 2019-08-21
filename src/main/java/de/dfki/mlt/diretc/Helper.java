/**
 *
 */
package de.dfki.mlt.diretc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import de.dfki.lt.tools.tokenizer.JTok;
import de.dfki.lt.tools.tokenizer.annotate.AnnotatedString;
import de.dfki.lt.tools.tokenizer.output.Outputter;
import de.dfki.lt.tools.tokenizer.output.Token;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class Helper {
	private JTok jtok;

	public Helper() {
		try {
			jtok = new JTok();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String fromStringToWikilabel(String label) {
		label = StringUtils.capitalize(label.trim().replaceAll(" ", "_")).trim();
		return label;
	}

	public static String fromLabelToKey(String label) {
		String key = StringUtils.deleteWhitespace((label.trim().replaceAll(" ", "_")));
		key = StringUtils.lowerCase(key);
		return key;
	}

	/**
	 * Returns the list of objects. Subject: ''' abc ''' = single token. object: [[
	 * abc ]] = single token. The positions are counted based on this schema.
	 **/
	public List<Word> getWordList(String text, String lang) {
		List<Word> words = new ArrayList<Word>();
		AnnotatedString annotatedString = jtok.tokenize(text, lang);
		List<Token> tokens = Outputter.createTokens(annotatedString);

		int index = 0;
		boolean inBracket = false;
		boolean isSubject = false;
		StringBuilder builder = null;
		for (Token token : tokens) {
			if (token.getImage().contains("'''") && !isSubject) {
				builder = build(token);
				isSubject = true;
			} else if (token.getImage().contains("'''") && isSubject) {
				isSubject = false;
				builder = append(builder, token);
				words.add(new Word(index, builder.toString().trim(), WordType.SUBJECT));
				index++;
			} else if (isSubject) {
				builder = append(builder, token);
			} else if (token.getType().equals("OCROCHE")) {
				builder = build(token);
				inBracket = true;
			} else if (token.getType().equals("CCROCHE") && inBracket) {
				builder = append(builder, token);
				words.add(new Word(index, builder.toString().trim(), WordType.OBJECT));
				inBracket = false;
				index++;
			} else if (inBracket) {
				builder = append(builder, token);
			} else {
				words.add(new Word(index, token.getImage(), WordType.OTHER));
				index++;
			}
		}
		return words;
	}

	public StringBuilder build(Token token) {
		StringBuilder builder = new StringBuilder();
		return append(builder, token);
	}

	public StringBuilder append(StringBuilder builder, Token token) {
		builder.append(token.getImage() + " ");
		return builder;
	}

	public String getCleanObject(String objSurface) {
		String cleanObjSurface = removeBracketsFromObject(objSurface);
		String objectSurface = cleanObjSurface;
		if (cleanObjSurface.contains("|")) {
			String[] objSurfaceArray = cleanObjSurface.split("\\|");
			if (objSurfaceArray.length > 1) {
				objectSurface = objSurfaceArray[1];
			} else {
				objectSurface = cleanObjSurface.replace("\\|", "");
			}
		}
		return objectSurface.trim();
	}

	public String getObjectEntryLabel(String objectSurface) {
		String label = removeBracketsFromObject(objectSurface);
		if (label.contains("|")) {
			String[] objSurfaceArray = label.split("\\|");
			if (objSurfaceArray.length > 1) {
				label = objSurfaceArray[0];
			} else {
				label = label.replace("\\|", "");
			}
		}
		label = fromStringToWikilabel(label);
		return label;
	}

	public String removeBracketsFromObject(String objectLabel) {
		objectLabel = objectLabel.replaceAll("\\[\\[", "").replaceAll("\\]\\]", "").trim();
		return objectLabel;
	}

}
