/**
 * 
 */
package de.dfki.mlt.freextractor;


import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class ElasticsearchServiceTest {

	public ElasticsearchService esService = new ElasticsearchService();

	@Test
	public void testGetMultiEntities() throws UnknownHostException {
		List<String> idList = new ArrayList<String>();
		idList.add("Q83030");
		List<Entity> results = esService.getMultiEntities(idList);
		List<HashMap<String, String>> actualClaims = results.get(0).getClaims();

		String[] expectedProperties = { "P1343", "P2176", "P279", "P1995", "P910", "P31" };
		List<String> expectedPropList = Arrays.asList(expectedProperties);
		String[] expectedItems = { "Q2657718", "Q409672", "Q3065932", "Q7867", "Q8372268", "Q12136" };
		List<String> expectedItemList = Arrays.asList(expectedItems);
		for (HashMap<String, String> claim : actualClaims) {
			if (claim.containsKey("property-id") && claim.containsKey("wikibase-item")) {
				assert (expectedPropList.contains(claim.get("property-id")));
				assert (expectedItemList.contains(claim.get("wikibase-item")));
			}
		}

	}

}
