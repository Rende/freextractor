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
	 * returns the list of objects, sets the subject position in the sentence.
	 * subject: ''' abc ''' = single token. object: [[ abc ]] = single token. the
	 * positions are counted based on this schema.
	 **/
	public List<SentenceItem> getSentenceItemList(String text) {
		List<SentenceItem> sentenceItemList = new ArrayList<SentenceItem>();
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
				sentenceItemList.add(new SentenceItem(index, builder.toString().trim(), Type.SUBJECT));
				index++;
			} else if (isSubject) {
				builder = append(builder, token);
			} else if (token.getType().equals("OCROCHE")) {
				builder = build(token);
				inBracket = true;
			} else if (token.getType().equals("CCROCHE") && inBracket) {
				builder = append(builder, token);
				sentenceItemList.add(new SentenceItem(index, builder.toString().trim(), Type.OBJECT));
				inBracket = false;
				index++;
			} else if (inBracket) {
				builder = append(builder, token);
			} else {
				sentenceItemList.add(new SentenceItem(index, token.getImage(), Type.OTHER));
				index++;
			}
		}
		return sentenceItemList;
	}

	public StringBuilder build(Token token) {
		StringBuilder builder = new StringBuilder();
		return append(builder, token);
	}

	public StringBuilder append(StringBuilder builder, Token token) {
		builder.append(token.getImage() + " ");
		return builder;
	}

	public String getLabel(String text) {
		String label = text.replaceAll("\\[\\[", "").replaceAll("\\]\\]", "").trim();
		if (label.contains("|")) {
			String[] labelArr = label.split("\\|");
			try {
				label = labelArr[0];
			} catch (ArrayIndexOutOfBoundsException e) {
				label = text.replaceAll("\\[\\[", "").trim().replace("\\|", "");
			}
		}
		label = Helper.fromStringToWikilabel(label);
		return label;
	}

}
