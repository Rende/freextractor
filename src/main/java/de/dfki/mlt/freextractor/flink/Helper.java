/**
 *
 */
package de.dfki.mlt.freextractor.flink;

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

	public static String fromStringToWikilabel(String image) {
		String label = "";
		if (image.contains("|")) {
			String[] images = image.split("\\|");
			try {
				label = images[0];
			} catch (ArrayIndexOutOfBoundsException e) {
				label = image.trim().replace("\\|", "");
			}
		} else {
			label = image;
		}
		label = StringUtils.capitalize(label.trim().replaceAll(" ", "_"));
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
	public List<Word> getWordList(String text) {
		List<Word> words = new ArrayList<Word>();
		AnnotatedString annotatedString = jtok.tokenize(text, "en");
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
				words.add(new Word(index, builder.toString().trim(), Type.SUBJECT));
				index++;
			} else if (isSubject) {
				builder = append(builder, token);
			} else if (token.getType().equals("OCROCHE")) {
				builder = build(token);
				inBracket = true;
			} else if (token.getType().equals("CCROCHE") && inBracket) {
				builder = append(builder, token);
				words.add(new Word(index, builder.toString().trim(), Type.OBJECT));
				inBracket = false;
				index++;
			} else if (inBracket) {
				builder = append(builder, token);
			} else {
				words.add(new Word(index, token.getImage(), Type.OTHER));
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

	public String getCleanObjectLabel(String objectSurface, boolean isOriginalLabel) {
		String label = cleanObjectLabel(objectSurface);
		if (label.contains("|")) {
			String[] objectSurfaceArray = label.split("\\|");
			if (isOriginalLabel) {
				try {
					label = objectSurfaceArray[0];
				} catch (ArrayIndexOutOfBoundsException e) {
					label = label.replaceAll("\\[\\[", "").trim().replace("\\|", "");
				}
				label = Helper.fromStringToWikilabel(label);
			} else {
				if (objectSurfaceArray.length > 1) {
					label = objectSurfaceArray[1];
				} else {
					label = label.replace("\\|", "");
				}
			}
		} else if (isOriginalLabel) {
			label = Helper.fromStringToWikilabel(label);
		}
		return label.trim();

	}

	public String cleanObjectLabel(String objectLabel) {
		objectLabel = objectLabel.replaceAll("\\[\\[", "").replaceAll("\\]\\]", "").trim();
		return objectLabel;
	}

}
