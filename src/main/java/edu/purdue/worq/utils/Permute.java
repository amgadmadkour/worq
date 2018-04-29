package edu.purdue.worq.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Amgad Madkour
 */
public class Permute {

	private List<List<String>> results;

	public Permute() {
		this.results = new ArrayList<>();
	}

	public List<List<String>> permute(List<String> arr, int k) {
		for (int i = k; i < arr.size(); i++) {
			java.util.Collections.swap(arr, i, k);
			permute(arr, k + 1);
			java.util.Collections.swap(arr, k, i);
		}
		if (k == arr.size() - 1) {
			this.results.add(Arrays.asList(arr.toArray(new String[arr.size()])));
		}
		if (k == 0) {
			return this.results;
		}
		return null;
	}
//	public static void main(String[] args){
//		//TESTING
//		Permute p = new Permute();
//		System.out.println(p.permute(java.util.Arrays.asList("dbr__mention","dbr__tweet","dbr__like"), 0));
//	}
}