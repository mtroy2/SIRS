<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="UTF-8"
    import="edu.nd.sirs.query.Matching"
    import="edu.nd.sirs.index.InvertedIndex"
    import="edu.nd.sirs.docs.Document"
    import="edu.nd.sirs.docs.TextDocument"
    import="edu.nd.sirs.index.DirectIndex"
    import="edu.nd.sirs.query.*"
    import="edu.nd.sirs.retrievalmodel.Boolean"
    import="edu.nd.sirs.retrievalmodel.BooleanScoreModifier"
     %>
<%!static int NUM_RESULTS_PER_PAGE = 10;
static int NEAREST_PAGES = 5;
static boolean SHOW_NEXT_PREV = true;

protected static void displayResults(ResultSet rs, int iStart, javax.servlet.jsp.JspWriter out) throws IOException
{
	
	float[] scores = rs.getScores();
	int[] docids = rs.getDocids();
	
	DirectIndex docStore = DirectIndex.getInstance();
	for(int i=0;i<rs.getResultSize() && i<NUM_RESULTS_PER_PAGE ;i++)
	{
		Document doc = docStore.getDoc(docids[i], TextDocument.class);
		final int rank = iStart + i + 1;
		out.print("<li value=\""+rank+"\" class=\"result\">");		
		out.print("<span class=\"results_rank\">"+ rank + "</span>");
		out.print("<span class=\"results\">"+doc.getName()+"</span>");
		out.print("<span class=\"results\">"+doc.getNumTokens()+"</span>");				
		out.print("<span class=\"results_score\">"+ scores[i] + "</span>");	
		out.println("</li>");
	}
}

protected static void displayPageNumbers(
		
		String q, ResultSet rs, 
		int iStart, javax.servlet.jsp.JspWriter out)
	throws IOException
{
	String sQuery_URLEncoded = java.net.URLEncoder.encode(q, "UTF-8");
	int maxResults = Math.min(rs.getExactResultSize(), 1000);//we dont let anyone go deeper than 1000
	int numPages = maxResults / NUM_RESULTS_PER_PAGE; 
	
	if (SHOW_NEXT_PREV && iStart > 0)
	{
		int prevStart = iStart - NUM_RESULTS_PER_PAGE;
		if (prevStart < 0)
			prevStart = 0;
		out.print("<a href=\"?query="+sQuery_URLEncoded+ "&start="+prevStart+"\">Previous</a> &nbsp;");
	}
	
	for(int i=0;i<numPages;i++)
	{
		int thisStart = (i * NUM_RESULTS_PER_PAGE);
		if (Math.abs(thisStart - iStart) > NUM_RESULTS_PER_PAGE * NEAREST_PAGES)
			continue;
		if (thisStart != iStart)
			out.print("<a href=\"?query="+sQuery_URLEncoded+ "&start="+ thisStart+"\">"+(i+1)+"</a>");
		else
			out.print(i+1);
		out.print("&nbsp;");
	}
	
	if (SHOW_NEXT_PREV)
	{
		int nextStart = iStart + NUM_RESULTS_PER_PAGE;
		if (nextStart < maxResults)
			out.print("<a href=\"?query="+sQuery_URLEncoded+ "&start="+nextStart+"\">Next</a>");
	}
}%>

<%
	String query = request.getParameter("query");
if (query == null || query.length() == 0)
	response.sendRedirect("./");
query = query.trim();
if (query == null || query.length() == 0)
	response.sendRedirect("./");
String sStart = request.getParameter("start");
int iStart;
if (sStart == null || sStart.length() == 0)
{
	sStart = "0";
	iStart = 0;
}
else
{
	iStart = Integer.parseInt(sStart);
	if (iStart > 1000)
	{
		iStart = 1000;
		sStart = "1000";
	}
}

InvertedIndex index = InvertedIndex.getInstance();
if (index == null)
{
	out.print("Error: no indox found.");
}

Matching m = new Matching(new BooleanRM());
m.addScoreModifier(new BooleanScoreModifier());
ResultSet rs = m.match(new Query(query));

int firstDisplayRank = iStart +1;
int lastDisplayRank = 1+ Math.min(rs.getExactResultSize() -1, iStart + NUM_RESULTS_PER_PAGE);
int possibleRanks = rs.getExactResultSize();
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<%@page import="java.io.IOException"%>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>SIRS Search results for <%=query%></title>
<link rel="stylesheet" type="text/css" href="style.css" />
</head>
<body>
<h1>Simple Information Retrieval System</h1>
<form action="results.jsp" id="queryform">
<input type="text" size="50" name="query" value="<%=query %>" />
<input type="submit"  value="Search" / >
</form>
<div id="summary">
Results for <%=query%>, displaying <%=firstDisplayRank%>-<%=lastDisplayRank%> of <%=possibleRanks %>
</div>
<ol id="results">
<%
displayResults(rs, iStart, out);
%>
</ol>
<div id="pages">
<%
displayPageNumbers(query, rs, iStart, out);
%>
</div>	
<hr width="50%">
</body>
</html>
