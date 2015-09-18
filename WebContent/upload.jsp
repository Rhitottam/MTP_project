<%@page import="solr_ingest.PostgresDatabaseManager"%>
<html>
<head>
<title>SIP/AIP Upload</title>
	<script type="text/javascript" src="js/jquery.js"></script>
	<script type="text/javascript" src="js/bootstrap.min.js"></script>
	<link href="css/autocomplete.css" rel="stylesheet">
	<script src="js/validator.js" type="text/javascript"></script>
	<script src="js/bootstrap-select.js" type="text/javascript"></script>
	<script src="js/typeahead.js" type="text/javascript"></script>
	<script src="https://gitcdn.github.io/bootstrap-toggle/2.2.0/js/bootstrap-toggle.min.js"></script>
	<script src="http://crypto-js.googlecode.com/svn/tags/3.0.2/build/rollups/md5.js"></script>
	<link href="https://gitcdn.github.io/bootstrap-toggle/2.2.0/css/bootstrap-toggle.min.css" rel="stylesheet">
	<link rel="stylesheet" type="text/css" href="css/bootstrap.min.css"/>
	<link rel="stylesheet" type="text/css" href="css/bootstrap-select.css"/>
	<link href='http://fonts.googleapis.com/css?family=Lato:400,700' rel='stylesheet' type='text/css'/>
	<style type="text/css">
		#main{
			position:relative;
			top: 100px;

			opacity: 0.9;
			z-index: 0;
			width: 900px;

  			margin: 0 auto;
  			overflow: visible;
  			height: auto;
  			font-family: "Lato";
  			font-weight: 400;
  			font-stretch: normal;
				background-color:white;

  			padding:30px;
			padding-top:10px;
  			border-radius: 4px;
  			box-shadow: 0px 10px 50px #26343F;
  			margin-bottom: 20px;
  			-webkit-transition: all 0.4s ease-in-out;
  			-moz-transition: all 0.4s ease-in-out;
  			transition: all 0.4s ease-in-out;

		}
		.input_file{
			padding:0 0 0 0;
			height:0px;
			width:30%;
			overflow:hidden;
		}
	</style>
	<script type="text/javascript">
	$(document).ready(function(){

		$(document).on("change","#input_sip",function(e){
			$("#click_upload_sip").html($("#input_sip").val());
		});
		$(document).on("click","#click_upload_sip",function(e){
			$("#input_sip").click();
		});

		$(document).on("change","#input_aip",function(e){
			$("#click_upload_aip").html($("#input_aip").val());
		});
		$(document).on("click","#click_upload_aip",function(e){
			$("#input_aip").click();
		});
		$("#form_sip").submit(function(e){
			//$("#input_aip").click();
			e.preventDefault();
			$("#submit_sip_dat").html('<div class="progress"><div class="progress-bar progress-bar-striped active" role="progressbar" aria-valuenow="45" aria-valuemin="0" aria-valuemax="100" style="width: 100%">Working....  </div></div>');
			var data = new FormData(this);
			$.ajax({
	            url: 'process_sip.jsp',
	            data: data,
	            cache: false,
	            contentType: false,
	            processData: false,
	            type: 'POST',     
	            success: function(data,status){
	            	$("#submit_sip_dat").html(data);
	            }
        	})
		});
		$("#form_aip").submit(function(e){
			//$("#input_aip").click();
			e.preventDefault();
			$("#submit_aip_dat").html('<div class="progress"><div class="progress-bar progress-bar-striped active" role="progressbar" aria-valuenow="45" aria-valuemin="0" aria-valuemax="100" style="width: 100%">Working....  </div></div>');
			var data = new FormData(this);
			$.ajax({
	            url: 'process_aip.jsp',
	            data: data,
	            cache: false,
	            contentType: false,
	            processData: false,
	            type: 'POST',     
	            success: function(data,status){
	            	$("#submit_aip_dat").html(data);
	            }
        	})
		});
		var substringMatcher = function(strs) {
		  return function findMatches(q, cb) {
		    var matches, substringRegex;

		    // an array that will be populated with substring matches
		    matches = [];

		    // regex used to determine if a string contains the substring `q`
		    substrRegex = new RegExp(q, 'i');

		    // iterate through the pool of strings and for any string that
		    // contains the substring `q`, add it to the `matches` array
		    $.each(strs, function(i, str) {
		      if (substrRegex.test(str)) {
		        matches.push(str);
		      }
		    });

		    cb(matches);
		  };
		};
		<%PostgresDatabaseManager.connect();
		out.println("var comms = "+PostgresDatabaseManager.getCommunities()+";");
		out.println("var colls = "+PostgresDatabaseManager.getCollections()+";");
		PostgresDatabaseManager.closeConnection();
		%>
		$("#comm_div .typeahead").typeahead({
			  hint: true,
			  highlight: true,
			  minLength: 1,
			  maxLength: 10
			},
			{
			  name: 'communities',
			  source: substringMatcher(comms)
			});
		
		$("#coll_div .typeahead").typeahead({
			  hint: true,
			  highlight: true,
			  minLength: 1,
			  maxLength: 10
			},
			{
			  name: 'collections',
			  source: substringMatcher(colls)
			});
		});

	</script>
</head>
<body>
<div id="main"><br>
<legend>SIP Input</legend>
<form action="" method="post" enctype="multipart/form-data" class="form" id="form_sip" name="form_sip">
	<div class="input-group" id="comm_div">
		<div class="input-group-addon" style="border-right:0px;"><span class="glyphicon glyphicon-folder-open" aria-hidden="true"></span></div>
		<input type="text" style="border-left:0px;min-width:250px;width:auto;" name = "community_name" class="form-control typeahead" id="community_name" placeholder="Community Path " required>
	</div><br>
	<div class="input-group" id="coll_div">
		<div class="input-group-addon" style="border-right:0px;"><span class="glyphicon glyphicon-folder-close" aria-hidden="true"></span></div>
		<input type="text" style="border-left:0px;min-width:250px;width:auto;" name = "collection_name" class="form-control typeahead" id="collection_name" placeholder="Collection Name " required>
	</div><br>
<div class="form-inline">

    <button type="button" id="click_upload_sip" class="btn btn-primary" style="background:inherit;color:blue;" >Upload input file</button>
    <span style="margin-left:10px;">Upload only SIP.zip files.</span>
    <input style="margin-left:10px;" type="submit" value="Submit" class="btn btn-default" id="submit_sip"/> 
    <input type="file" class="input_file" name="input_sip" id= "input_sip"/>
</div><br><div id = "submit_sip_dat">
	
</div>
</form>
<legend>AIP Input</legend>
<form action="process_aip.jsp" method="post" enctype="multipart/form-data" class="form form-inline" id="form_aip" name="form_aip">
<div class="form-inline">
    <button type="button" id="click_upload_aip" class="btn btn-primary" style="background:inherit;color:blue;" >Upload input file</button>
    <span style="margin-left:10px;">Upload only AIP files in tar.gz format.</span>
    <input style="margin-left:10px;" type="submit" value="Submit" class="btn btn-default"/> 
    <input type="file" class="input_file" name="input_file" id="input_aip"/>     
</div><br>
<div id = "submit_aip_dat">
	
</div>
</form> 
</div>
</body>
</html>