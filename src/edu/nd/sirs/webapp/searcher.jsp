<%@ page language="java"%>
<%@ page contentType="application/json"%>
<%@ page pageEncoding="UTF-8"%>
<%@ page import="edu.nd.sirs.query.*"%>
<%@ page import="edu.nd.sirs.index.*"%>
<%@ page import="java.net.*"%>
<%@ page import="java.util.HashMap"%>
<%@ page import="edu.nd.sirs.docs.*"%>
<%@ page import="edu.nd.sirs.util.*"%>
<%@ page import="edu.nd.sirs.eval.*"%>
<%@ page import="edu.nd.sirs.retrievalmodel.*"%>

<%
	long time = System.currentTimeMillis();

	// Returns all employees (active and terminated) as json.
	response.setContentType("application/json");
	response.setHeader("Content-Disposition", "inline");

	/* Receive GET paramenters */
	String model = request.getParameter("model");
	String query = request.getParameter("query");
	int bodyWgt = Integer.parseInt(request.getParameter("bodywgt"));
	int linkWgt = Integer.parseInt(request.getParameter("linkwgt"));
	int titleWgt = Integer.parseInt(request.getParameter("titlewgt"));
	
	HashMap<String, Float> wgts = new HashMap<String, Float>(3);
	wgts.put("body", (float) bodyWgt);
	wgts.put("link", (float) linkWgt);
	wgts.put("title", (float) titleWgt);

	ResultSet rs = null;

	Matching m = null;
%>
<%
	switch (model) {
	case "Boolean":
		m = new Matching(new BooleanRM());
		Fields.getInstance().assignWeights(wgts);
		m.addScoreModifier(new BooleanScoreModifier());
		rs = m.match(new Query(query));
		break;
	case "Cosine":
		m = new Matching(new CosineRM());
		Fields.getInstance().assignWeights(wgts);
		m.addScoreModifier(new CosineScoreModifier());
		rs = m.match(new Query(query));
		break;
	}

	StringBuffer json_r = new StringBuffer();

	Evaluate g = new Evaluate();
	EvaluationResults er = g.evaluate(rs, query, 10);

	for (int i = 0; i < rs.getResultSize(); i++) {
		int docid = rs.getDocids()[i];
		HTMLDocument doc = (HTMLDocument) DirectIndex.getInstance()
				.getDoc(docid, HTMLDocument.class);
		Object title = doc.getResources().get("title");
		if(title == null){
			title = "";
		}
		json_r.append("{\"title\":\"" + title.toString().replaceAll("\"", "")
				+ "\",\"docid\":\"" + doc.getDocId()
				+ "\",\"url\":\"" + doc.getName() + "\"}");

		if (i < (rs.getResultSize() - 1)) {
			json_r.append(",");
		}
	}
%>
<%
	String json = "{\"size\":" + rs.getResultSize() + ",\"time\":\""
			+ (System.currentTimeMillis() - time) + "\",\"data\":["
					+ json_r.toString() + "],\"eval\":["
			+ er.toJSON() + "]}\r\n";

	out.write(json);
%>