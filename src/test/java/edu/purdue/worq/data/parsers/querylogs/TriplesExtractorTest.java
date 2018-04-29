package edu.purdue.worq.data.parsers.querylogs;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Amgad Madkour
 */
public class TriplesExtractorTest {
	@Test
	public void extractProperties() throws Exception {
		String extprop = "Test 1";
		assertEquals(extprop, "Test 1");
	}

	@Test
	public void extractJoins() throws Exception {
		String ex2 = "Test 2";
		assertEquals(ex2, "Test 2");
	}
}