package edu.purdue.worq.queryprocessing.execution;

/**
 * @author Amgad Madkour
 */
public class WORQResult {
	private long numResults;
	private double time;
	private long tableSizes;
	private long reductionSizes;
	private int maxJoins;
	private boolean isWarm;
	private int numTriples;

	public WORQResult(long numResults, double time, long tableSizes, long reductionSizes, int maxJoins, boolean isWarm, int numTriples) {
		this.numResults = numResults;
		this.time = time;
		this.tableSizes = tableSizes;
		this.reductionSizes = reductionSizes;
		this.maxJoins = maxJoins;
		this.isWarm = isWarm;
		this.numTriples = numTriples;
	}

	public long getNumResults() {
		return numResults;
	}

	public void setNumResults(int numResults) {
		this.numResults = numResults;
	}

	public double getTime() {
		return time;
	}

	public void setTime(double time) {
		this.time = time;
	}

	public long getTableSizes() {
		return tableSizes;
	}

	public void setTableSizes(long tableSizes) {
		this.tableSizes = tableSizes;
	}

	public long getReductionSizes() {
		return reductionSizes;
	}

	public void setReductionSizes(long reductionSizes) {
		this.reductionSizes = reductionSizes;
	}


	public int getMaxNumJoins() {
		return maxJoins;
	}

	public void setMaxJoins(int maxJoins) {
		this.maxJoins = maxJoins;
	}

    public boolean isWarm() {
        return isWarm;
    }

    public void setWarm(boolean warm) {
        isWarm = warm;
    }

	public int getNumTriples() {
		return numTriples;
	}

	public void setNumTriples(int numTriples) {
		this.numTriples = numTriples;
	}
}
