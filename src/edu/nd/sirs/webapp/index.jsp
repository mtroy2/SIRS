<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<!-- <meta http-equiv="X-UA-Compatible" content="IE=edge"> -->
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>Simple Information Retrieval System</title>

<!-- Bootstrap -->
<link href="bootstrap.css" rel="stylesheet">
<link href="sticky-footer.css" rel="stylesheet">

<!-- HTML5 Shim and Respond.js IE8 support of HTML5 elements and media queries -->
<!-- WARNING: Respond.js doesn't work if you view the page via file:// -->
<!--[if lt IE 9]>
      <script src="https://oss.maxcdn.com/libs/html5shiv/3.7.0/html5shiv.js"></script>
      <script src="https://oss.maxcdn.com/libs/respond.js/1.4.2/respond.min.js"></script>
    <![endif]-->
</head>
<body>

	<!-- Begin page content --> 
	<div class="container">
		<div class="page-header">
			<h1>
				<img src="ND_monogram_blue_S.png" /> Simple Information Retrieval System
			</h1>
		</div>
	</div>

	<div class="container">
		<form role="form">
			<div class="form-group">
				<div class="input-group">
					<div class="input-group-btn">
						<button id="model" type="button"
							class="btn btn-primary dropdown-toggle" data-toggle="dropdown">
							<span class="selection">Boolean</span>  
							<span class="caret"></span>
						</button>
						<ul class="dropdown-menu">
							<li><a href="#">Boolean</a></li>							
							<li><a href="#">Cosine</a></li>
							<li><a href="#">Cosine + PageRank</a></li>
							<li><a href="#">BM25</a></li>
							<li><a href="#">BM25 + Cosine</a></li>
							<li><a href="#">BM25 + PageRank</a></li>
							<li><a href="#">PageRank</a></li>
						</ul>
					</div>
					<!-- /btn-group -->
					<input id="query" type="text" class="form-control"> <span
						class="input-group-btn">
						<button id="search" class="btn btn-primary" type="button">
							<span class="glyphicon glyphicon-search"></span>
						</button>
					</span>
				</div>
				<!-- /input-group -->
			</div>
		</form>
	</div>

	<div class="container">
		<p id="tmp"></p>
	</div>
	
	<div id="stats_container" class="container">
		<p></p>
	</div>

	<div id="result_container" class="container results-container">
	</div>

	<div id="footer">
		<div class="container">
			<p class="text-muted">
			</center>
			University of Notre Dame - Tim Weninger
			</p>
		</div>
	</div>


	<!-- jQuery -->
	<script src="jquery-1.11.2.js"></script>
	<!-- Bootstrap -->
	<script src="bootstrap.js"></script>

	<script src="search.js"></script>
</body>
</html>
