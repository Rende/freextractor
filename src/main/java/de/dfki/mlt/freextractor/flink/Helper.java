/**
 *
 */
package de.dfki.mlt.freextractor.flink;

import org.apache.commons.lang.StringUtils;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class Helper {
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
		String key = StringUtils.deleteWhitespace((label.trim().replaceAll(" ",
				"_")));
		key = StringUtils.lowerCase(key);
		return key;
	}

}
