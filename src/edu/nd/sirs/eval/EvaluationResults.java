package edu.nd.sirs.eval;

public class EvaluationResults {

	private int missing;
	private float prec;
	private float rec;
	private float f1;
	private float avgprec;
	private float mrr;
	private float ndcg;

	public EvaluationResults(int missing, float sumPrec, float sumRecall,
			float sumF1, float sumAvgPrec, float sumMRR, float sumNDCG) {
		this.missing = missing;
		this.prec = sumPrec;
		this.rec = sumRecall;
		this.f1 = sumF1;
		this.avgprec = sumAvgPrec;
		this.mrr = sumMRR;
		this.ndcg = sumNDCG;
	}
	
	public String toJSON(){
		return "{\"missing\":\"" + missing
				+ "\",\"precision\":\"" + prec  
				+ "\",\"recall\":\"" + rec  
				+ "\",\"f1\":\"" + f1  
				+ "\",\"avgprec\":\"" + avgprec  
				+ "\",\"mrr\":\"" + mrr  			
				+ "\",\"ndcg\":\"" + ndcg + "\"}";
	}

}
