from SPARQLWrapper import SPARQLWrapper2
import math
import os
from pprint import pprint

if os.path.isdir("/gridappsd/services/gridappsd-sensor-simulator"):
    blazegraph_url = "http://blazegraph:8080/bigdata/sparql"
else:
    blazegraph_url = "http://localhost:8889/bigdata/sparql"

cim100 = '<http://iec.ch/TC57/CIM100#'
# Prefix for all queries.
prefix = """PREFIX r: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX c: {cimURL}>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
""".format(cimURL=cim100)

sparql = SPARQLWrapper2(blazegraph_url)

ROOT_3 = math.sqrt(3)


def get_measurements(feeder):
    query = """
    # list all measurements, with buses and equipments - DistMeasurement
    SELECT ?class ?type ?name ?bus ?phases ?eqtype ?eqname ?eqid ?trmid ?id ?cnid WHERE {
        VALUES ?fdrid {"%s"}
         ?eq c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid. 
        { ?s r:type c:Discrete. bind ("Discrete" as ?class)}
          UNION
        { ?s r:type c:Analog. bind ("Analog" as ?class)}
         ?s c:IdentifiedObject.name ?name .
         ?s c:IdentifiedObject.mRID ?id .
         ?s c:Measurement.PowerSystemResource ?eq .
         ?s c:Measurement.Terminal ?trm .
         ?s c:Measurement.measurementType ?type .
         ?trm c:IdentifiedObject.mRID ?trmid.
         ?eq c:IdentifiedObject.mRID ?eqid.
         ?eq c:IdentifiedObject.name ?eqname.
         ?eq r:type ?typeraw.
          bind(strafter(str(?typeraw),"#") as ?eqtype)
         ?trm c:Terminal.ConnectivityNode ?cn.
         ?cn c:IdentifiedObject.name ?bus.
          ?cn c:IdentifiedObject.mRID ?cnid.
         ?s c:Measurement.phases ?phsraw .
           {bind(strafter(str(?phsraw),"PhaseCode.") as ?phases)}
} ORDER BY ?class ?type ?name
""" % (feeder, )

    query = prefix + query
    sparql.setQuery(query)
    ret = sparql.query()
    results = {}

    for r in ret.bindings:
        data = {'class': r['class'].value,
                'type': r['type'].value,
                'cnid': r['cnid'].value,
                "measurement_mrid": r['id'].value}
        results[r['id'].value] = data

    return results


def get_cn_nomv(fdr):
    query = """
    ## list all the connectivity node base voltages by feeder, for sensor service
    SELECT DISTINCT ?feeder ?busname ?cnid ?nomv WHERE {
        VALUES ?fdrid {"%s"}
        ?fdr c:IdentifiedObject.mRID ?fdrid.
        ?bus c:ConnectivityNode.ConnectivityNodeContainer ?fdr.
        ?bus r:type c:ConnectivityNode.
        ?bus c:IdentifiedObject.name ?busname.
        ?bus c:IdentifiedObject.mRID ?cnid.
        ?fdr c:IdentifiedObject.name ?feeder.
        ?trm c:Terminal.ConnectivityNode ?bus.
        ?trm c:Terminal.ConductingEquipment ?ce.
        ?ce  c:ConductingEquipment.BaseVoltage ?bv.
        ?bv  c:BaseVoltage.nominalVoltage ?nomv.
    }
    ORDER by ?feeder ?busname ?nomv
""" % (fdr, )

    query = prefix + query
    sparql.setQuery(query)
    ret = sparql.query()
    #print(f"VARS are: {ret.variables}")
    results = {}
    class_set = set()
    eq_set = set()
    cn_nomv = {}
    for r in ret.bindings:
        results[r['cnid'].value] = float(r['nomv'].value) / ROOT_3
        #results[r['id'].value] = {'class': r['class'].value, 'eqid': r['eqid'].value}
        #class_set.add(r['class'].value)
        #eq_set.add(r['eqid'].value)
    # print(f"Found {len(results)} items.")
    # print(f"Set is: {class_set}")
    # print(f"eq set is: {eq_set}")
    return results


def get_fullscale_current(feeder: str, cn_nomv: dict):
    query = """
    # list all the CurrentLimits for sensor service nominal values
    SELECT ?eqtype ?eqname ?eqid ?val ?cn1id WHERE {
        VALUES ?dur {"5E9"}
        VALUES ?seq {"1"}
        VALUES ?fdrid {"%s"}
        ?fdr c:IdentifiedObject.mRID ?fdrid.
        ?eq c:Equipment.EquipmentContainer ?fdr.
        ?eq c:IdentifiedObject.name ?eqname.
        ?eq c:IdentifiedObject.mRID ?eqid.
        ?eq r:type ?rawtype.
          bind(strafter(str(?rawtype),"#") as ?eqtype)
        ?t c:Terminal.ConductingEquipment ?eq.
        ?t c:ACDCTerminal.OperationalLimitSet ?ols.
        ?t c:ACDCTerminal.sequenceNumber ?seq.
        ?t c:Terminal.ConnectivityNode ?cn1.
        ?cn1 c:IdentifiedObject.mRID ?cn1id.
        ?clim c:OperationalLimit.OperationalLimitSet ?ols.
        ?clim r:type c:CurrentLimit.
        ?clim c:OperationalLimit.OperationalLimitType ?olt.
        ?olt c:OperationalLimitType.acceptableDuration ?dur.
        ?clim c:CurrentLimit.value ?val.
    }
    ORDER by ?eqtype ?eqname ?eqid ?val
""" % (feeder, )
    results = {}
    query = prefix + query
    sparql.setQuery(query)
    ret = sparql.query()

    print("Missing Values")
    for r in ret.bindings:
        #print(r)
        try:
            data = dict(eqtype=r['eqtype'].value, eqname=r['eqname'].value,
                        eqid=r['eqid'].value, val=r['val'].value, cn1id=r['cn1id'].value,
                        power=float(r['val'].value) * cn_nomv[r['cn1id'].value])
            results[r['cn1id'].value] = data
        except KeyError as e:
            print(f'{r}')
            #print(e.args)

    return results


def get_nomv(fdr, eqset):
    query = """
    SELECT (xsd:float(?vnom) AS ?vnomvoltage) ?id WHERE {
        VALUES ?fdrid { "%s" }
        ?s c:ConnectivityNode.ConnectivityNodeContainer|c:Equipment.EquipmentContainer ?fdr. 
        ?s c:ConductingEquipment.BaseVoltage ?lev.
        ?s c:IdentifiedObject.mRID ?id . 
        ?lev c:BaseVoltage.nominalVoltage ?vnom.
    }
"""

    query = prefix + query
    print(query)
    sparql.setQuery(query)
    ret = sparql.query()
    # print(f"VARS are: {ret.variables}")
    results = {}
    class_set = set()

    results = {}
    for r in ret.bindings:
        if r['id'].value in eqset:
            #if r['class']== 'Discrete':
            #    results[r['id'].value] = float(r['vnomvoltage'].value)
            #elif r['class'] == 'Analog':
            results[r['id'].value] = float(r['vnomvoltage'].value) / ROOT_3
            #else:
            #    raise AttributeError(f"Invalid class found in {r['class']}")

    return results


def get_sensors_config(fdr):
    # get measurement dictionary and set of equipment that is for measurments
    nomv_by_cnid = get_cn_nomv(fdr)
    cn1_power = get_fullscale_current(fdr, nomv_by_cnid)
    # # get nominal voltage across the equipment set of id
    # vnoms = get_vnoms(fdr, eqset)
    #
    # # finally loop over measurements list and add nominal voltageg (normal_value)
    # # for the configuration of the measurement.
    #
    # for k, v in measurements.items():
    #     if v['eqid'] in vnoms:
    #         v['normal_value'] = vnoms[v['eqid']]
    #
    return nomv_by_cnid, cn1_power


if __name__ == '__main__':
    import json
    fdr_9500 = "_AAE94E4A-2465-6F5E-37B1-3E72183A4E44"
    #nomv_cnid, cn1_power = get_sensors_config(fdr_9500)

    my_measurements = get_measurements(fdr_9500)
    cn_nomv = get_cn_nomv(fdr_9500)
    current_nom_power = get_fullscale_current(fdr_9500, cn_nomv)
    print()
    print("Missing Measurements")
    for k, v in my_measurements.items():
        try:
            v['cn_nomv'] = cn_nomv[v['cnid']]
        except KeyError:
            print(f"{v}")
        # try:
        #     v['current_nomv'] = current_nom_power[v['cnid']]
        # except KeyError:
        #     print(f"current_nomv key error for {v}")

    with open("/home/osboxes/repos/gridappsd-python-log/new_measurement_data.json", 'w') as fp:
        fp.write(json.dumps(my_measurements, indent=4))


    # with open("/home/osboxes/repos/gridappsd-python-log/measurement_first.json") as fp:
    #     measurement_data = json.load(fp)
    # #
    # # lst_meeasurements = get_measurements(fdr_9500)
    # full = set(measurement_data.keys())
    # found = set()
    # for k in measurement_data:
    #     if k in my_measurements:
    #         print(f"Found {k}")
    #         found.add(k)
    #     else:
    #         print(f"Not found {k}")
    #     # if k in nomv_cnid:
    #     #     print(f"Found {k}")
    #     # else:
    #     #     print(f"NOT FOUND {k}")
    #
    # print(f"Missing: {full - found}")
    # print(f"Other missing: {found - full}")

    #pprint(sensors)
