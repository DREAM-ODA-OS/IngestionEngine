


function goTolocation(s)
{
	var main_page = new String("http://127.0.0.1:8000/")
	var page = main_page.concat(s);
	window.location.replace(page)
}


function goToEditScenario()
{
window.location.replace("http://127.0.0.1:8000/editScenarioForms/");
}

