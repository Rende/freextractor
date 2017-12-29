/**
 *
 */
package de.dfki.mlt.freextractor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import de.dfki.mlt.freextractor.flink.Entity;
import de.dfki.mlt.freextractor.flink.RelationExtractionMap;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class RelationExtractionMapTest {

	public RelationExtractionMapTest() {

	}

	@Test
	public void test() throws IOException {

		RelationExtractionMap map = new RelationExtractionMap();
		String[] tokens = { "''' Krini '''", "is", "a", "village", "in", "the",
				"municipal", "unit", "of",
				"[[ Oichalia , Trikala | Oichalia ]]", "in", "the",
				"southeastern", "part", "of", "the",
				"[[ Trikala | Trikala regional unit ]]", ",", "[[ Greece ]]",
				"." };
		String[] aliases = { "was a", "of Oichalia in" };

		List<String> tokenList = Arrays.asList(tokens);
		Entity property = new Entity("P31", "property", "instance of", "",
				Arrays.asList(aliases),null);
//		Relation relation = map.searchRelation(tokenList, property, 5, "", "", 234);
//		assertThat(relation.getSurface()).isEqualTo(
//				"of [[ Oichalia , Trikala | Oichalia ]] in");

	}
}
