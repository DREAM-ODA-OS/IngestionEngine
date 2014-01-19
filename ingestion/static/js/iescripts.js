/*
 *  Project: DREAM
 *  Module:  Task 5 ODA Ingestion Engine 
 *  Authors: Vojtech Stefka  (CVC), Milan Novacek (CVC)
 *
 *    (c) 2013 Siemens Convergence Creators s.r.o., Prague
 *    Licensed under the 'DREAM ODA Ingestion Engine Open License'
 *     (see the file 'LICENSE' in the top-level directory)
 *
 *  Ingestion Admin Client javascript functions.
 *
 */

/* ----------------- overviewScnenario.html --------------------- */

function get_scenario(ncn_id){
    for (var i=0; i<jscenarios.length; i++) {
        if (ncn_id==jscenarios[i].ncn_id) {
            return jscenarios[i];
        }
    }
    return null;
}

function run_delete(ncn_id) {
    // update status of scenario
    sync_scenarios();
    confirm_text =
        "This will delete all scenario data\n" +
        "and settings, and de-register all\n" +
        "downloaded products from the ODA server.";
    scenario = get_scenario(ncn_id);
    if (scenario && scenario.st_isav!=0) {
        if(confirm
           ('Delete scenario - ' + ncn_id + '?\n' +
            confirm_text )) {
            // send ajax request to Work-Flow-Manager to delete scenario
            Dajaxice.ingestion.delete_scenario_wfm
                (function(data){
                    if(data.status !== undefined) {
                        if (data.status != 0) { alert(data.message); }
                    }
                },
                {'ncn_id': ncn_id});
            // ensure updates of the page via sychronize_scenario
            operation_pending = true;
            was_active = true;
            sync_scenarios();
        }
    }
}

function ingest(ncn_id){
    scenario = get_scenario(ncn_id);
    if (null == scenario) {
        alert("No scenario found: internal error" +
              "Please reload the page");
        return;
    }
    if (scenario.st_isav==0) {
        alert('Scenario '+ncn_id+
              'is locked - operation in progress');
    } else {
        if(confirm('Ingest scenario '+ncn_id+'?')) {
            // send ajax request to Work-Flow-Manager to ingest scenario
            Dajaxice.ingestion.ingest_scenario_wfm
                (function(data){
                    if(data.status !== undefined) {
                        if (data.status != 0) { alert(data.message); }
                    }
                },
                {"ncn_id": ncn_id});
        }
    } // closes else
}

function run_ingestion(ncnid_scenario) {

    Dajaxice.ingestion.synchronize_scenarios
        (function(data) {
            update_scenario(data);
            ingest(data.op_sc);
        },
        {"scenario_id": ncnid_scenario} );
    // ensure updates of the page via sychronize_scenario
    operation_pending = true;
    sync_scenarios();
}

function stop_ingestion(s)
{
    alert("stopping ingestion, scnenario: "+s);
    Dajaxice.ingestion.stop_ingestion_wfm(
        function(data){
            if(data.status !== undefined) {
                if (data.status != 0) {
                    alert(data.message)
                        }
            }
        },
        {"scenario_id":s});
    Dajaxice.ingestion.synchronize_scenarios(update_scenario);
    operation_pending = false;
    was_active = false;
}

function goToLocation(s)
{
    var new_page = "http://".concat(window.location.host,"/",s);
    window.location.replace(new_page)
}

function create_scenario(data) {
    //     0         1          2        3            4          5      6
    //scenario_id,ncn_id,status_id,status_is_available,status_status,status_done
    // create scenario as javascript object
    var scenario = new Object();
    scenario.id     = data[0];
    scenario.ncn_id = data[1];

    if (data[2]>0) { // repeat inteval==0 means manual
        scenario.auto_ingest = 1;
    }else {
        scenario.auto_ingest = 0;
    }

    scenario.st_id   = data[3];
    scenario.st_isav = data[4];
    scenario.st_st   = data[5];
    scenario.st_done = data[6];
    return scenario;
}

function add_scenario(scenario) {
    jscenarios.push(scenario);
}

function remove_scenario(scenario) {
    var i = jscenarios.indexOf(scenario)
    splice (i,i);
}

function update_scenario(data) {
    // called by dajaxice every N-milliseconds
    // and also by various other operations
    if (data === null)
    {
        alert("update_scenario: data is null");
        return;
    }

    // First set status of all scenarios to 'deleted' and 'disabled'
    // The ones that are still alive will get reset
    // to something else in the next step
    if (! first_time){
        for (var i=0; i<jscenarios.length; i++) {
            jscenarios[i].st_st = "DELETED";
            jscenarios[i].st_isav=0;
            jscenarios[i].st_done=0;
        }
    }
    // now update the status of all still existing scenarios.
    var ajax_data = data.jscenario_status;
    for (var i=0; i<ajax_data.length; i++) {
        var obj = create_scenario(ajax_data[i]);
        if (first_time==1){
            jscenarios.push(obj);
        }else {
            jscenarios[i] = obj;
        }
    }
    if (first_time==1){
        first_time = 0;
    }
    // update widgets (buttons,progress bars, ...)
    update_oveview_page();

    /*
    for (var i=0; i<jscenarios.length; i++) {
        if (jscenarios[i].st_st=="DELETED") {
            Dajaxice.ingestion.delete_scenario_django(
                function(data){alert(data.message);},
                {'scenario_id':jscenarios[i].id});
        }
    }
    */
}

function update_oveview_page() {
    // update widgets of Web page
    for (var i=0; i<jscenarios.length; i++) {
                   
        // hide div_scenario of deleted scenario
        if (jscenarios[i].st_st=="DELETED") {
            current_id = "#div_scenario_" + jscenarios[i].ncn_id;
            $(current_id).hide();
        }

        // update tr_status
        current_id = "div_status_" + jscenarios[i].ncn_id;
        var element = document.getElementById(current_id);
        element.innerHTML = " " + jscenarios[i].st_st;

        // update progress bar
        done = jscenarios[i].st_done;
        el = document.getElementById("progress_bar_"+jscenarios[i].ncn_id);
        el.style.width = done.toString() + "%";
        // if use jqueryui, instead of the above do this:
        //    current_id = "#div_progress_bar_" + jscenarios[i].id;
        //    $(current_id).progressbar({ value: done });

        //  enable/disable buttons
        buttons = [
           "ingest_scenario_button",
           "ingest_local_product_button",
           //               "manage_data_button",
           //               "load_register_button",
           "edit_scenario_button",
           "delete_scenario_button"
           ];
        for (var j=0; j<buttons.length; j++) {
            //current_id = "#" + buttons[j] + "_" + jscenarios[i].id;
            current_id = buttons[j] + "_" + jscenarios[i].ncn_id;
            if (jscenarios[i].st_isav==0) { // disable
                document.getElementById(current_id).disabled = true;
            }else { // enable
                document.getElementById(current_id).disabled = false;
            }

            if (buttons[j]=="ingest_button") {
                if (jscenarios[i].auto_ingest==1) {
                    document.getElementById(current_id).disabled = true;
                }
            }
        }
    }
}

function sync_scenarios() {
   Dajaxice.ingestion.synchronize_scenarios(update_scenario);
}


