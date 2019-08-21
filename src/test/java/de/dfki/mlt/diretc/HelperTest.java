package de.dfki.mlt.diretc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

import de.dfki.mlt.diretc.Helper;
import de.dfki.mlt.diretc.Word;
import de.dfki.mlt.diretc.WordType;

public class HelperTest {
	private Helper helper = new Helper();
	private String lang = "en";

	@Test
	public void testGetWordList() {
		String test = "''' Saint-Esteben ''' be a [[ commune of France | commune ]] in "
				+ "the [[ pyrénées-atlantique ]] [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";
		List<Word> actualList = helper.getWordList(test, this.lang);
		assertThat(actualList).extracting("position").containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
		assertThat(actualList).extracting("surface").containsExactly("''' Saint-Esteben '''", "be", "a",
				"[[ commune of France | commune ]]", "in", "the", "[[ pyrénées-atlantique ]]",
				"[[ Departments of France | department ]]", "in", "south-western", "[[ France ]]", ".");
		assertThat(actualList).extracting("type").containsExactly(WordType.SUBJECT, WordType.OTHER, WordType.OTHER, WordType.OBJECT,
				WordType.OTHER, WordType.OTHER, WordType.OBJECT, WordType.OBJECT, WordType.OTHER, WordType.OTHER, WordType.OBJECT, WordType.OTHER);
	}

	@Test
	public void testGetCleanObjectLabel() {
		String test = "[[ sonata#the baroque sonata | sonata ]]";
		String expectedSurface = "sonata";
		String actualSurface = helper.getCleanObject(test);
		assertThat(actualSurface).isEqualTo(expectedSurface);

		String expectedLabel = "Sonata#the_baroque_sonata";
		String actualLabel = helper.getObjectEntryLabel(test);
		assertThat(actualLabel).isEqualTo(expectedLabel);
	}

}
