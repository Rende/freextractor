package de.dfki.mlt.freextractor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

import de.dfki.mlt.freextractor.flink.Helper;
import de.dfki.mlt.freextractor.flink.Type;
import de.dfki.mlt.freextractor.flink.Word;

public class HelperTest {
	private Helper helper = new Helper();
	
	@Test
	public void testGetWordList() {
		String test = "''' Saint-Esteben ''' be a [[ commune of France | commune ]] in "
				+ "the [[ pyrénées-atlantique ]] [[ Departments of France | department ]] "
				+ "in south-western [[ France ]] .";
		List<Word> actualList = helper.getWordList(test);
		assertThat(actualList).extracting("position").containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
		assertThat(actualList).extracting("surface").containsExactly("''' Saint-Esteben '''", "be", "a",
				"[[ commune of France | commune ]]", "in", "the", "[[ pyrénées-atlantique ]]",
				"[[ Departments of France | department ]]", "in", "south-western", "[[ France ]]", ".");
		assertThat(actualList).extracting("type").containsExactly(Type.SUBJECT, Type.OTHER, Type.OTHER, Type.OBJECT,
				Type.OTHER, Type.OTHER, Type.OBJECT, Type.OBJECT, Type.OTHER, Type.OTHER, Type.OBJECT, Type.OTHER);
	}
	
	@Test
	public void testGetCleanObjectLabel() {
		String test = "[[ abc xyz | def ]]";
		String expectedSurface = "def";
		String actualSurface = helper.getCleanObjectLabel(test, false);
		assertThat(actualSurface).isEqualTo(expectedSurface);
		
		String expectedLabel = "Abc_xyz";
		String actualLabel = helper.getCleanObjectLabel(test, true);
		assertThat(actualLabel).isEqualTo(expectedLabel);
	}


}
