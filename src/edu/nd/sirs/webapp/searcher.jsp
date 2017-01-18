<%@ page language="java"%>
<%@ page contentType="application/json"%>
<%@ page pageEncoding="UTF-8"%>
<%@ page import="edu.nd.sirs.query.*"%>
<%@ page import="edu.nd.sirs.index.*"%>
<%@ page import="edu.nd.sirs.docs.*"%>
<%@ page import="edu.nd.sirs.retrievalmodel.*"%>

<%
	long time = System.currentTimeMillis();

	// Returns all employees (active and terminated) as json.
	response.setContentType("application/json");
	response.setHeader("Content-Disposition", "inline");

	/* Receive GET paramenters */
	String model = request.getParameter("model");
	String query = request.getParameter("query");

	ResultSet rs = null;

	Matching m = null;
%>
<%
	switch (model) {
	case "Boolean":
		m = new Matching(new BooleanRM());
		m.addScoreModifier(new BooleanScoreModifier());
		rs = m.match(new Query(query));
		break;
	}

	StringBuffer json_r = new StringBuffer();
	%><%
	for (int i=0; i<rs.getResultSize(); i++) {
		int docid = rs.getDocids()[i];
		TextDocument doc = (TextDocument) DirectIndex.getInstance()
				.getDoc(docid, TextDocument.class);
		json_r.append("{\"title\":\"" + doc.getDocId()
				+ "\",\"url\":\"" + doc.getName() + "\"}");

		if (i < (rs.getResultSize()-1)) {
			json_r.append(",");
		}
	}
	
	%><%

	String json = "{\"size\":" + rs.getResultSize() + ",\"time\":\""
			+ (System.currentTimeMillis() - time) + "\",\"data\":["
			+ json_r.toString() + "]}\r\n";

	out.write(json);
%>