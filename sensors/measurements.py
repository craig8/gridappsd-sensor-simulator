from SPARQLWrapper import SPARQLWrapper2
from pprint import pprint

cim100 = '<http://iec.ch/TC57/CIM100#'
# Prefix for all queries.
prefix = """PREFIX r: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX c: {cimURL}>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
""".format(cimURL=cim100)

blazegraph_url = "http://localhost:8889/bigdata/sparql"
sparql = SPARQLWrapper2(blazegraph_url)


def get_measurements(fdr):
    query = """
    # list all measurements, with buses and equipments - DistMeasurement
    SELECT ?class ?type ?name ?bus ?phases ?eqtype ?eqname ?eqid ?trmid ?id WHERE {
    VALUES ?fdrid {"%s"}
    # VALUES ?id {"_0000ff53-9ecf-442e-84d1-33395ea819a1"}
    # VALUES ?id {"_ee050c5b-c0ba-46d3-b310-c40eedb262ca"}
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
     ?s c:Measurement.phases ?phsraw .
       {bind(strafter(str(?phsraw),"PhaseCode.") as ?phases)}
    } ORDER BY ?class ?type ?name""" % (fdr, )

    query = prefix + query
    sparql.setQuery(query)
    ret = sparql.query()
    #print(f"VARS are: {ret.variables}")
    results = {}
    class_set = set()
    eq_set = set()
    for r in ret.bindings:
        results[r['id'].value] = {'class': r['class'].value, 'eqid': r['eqid'].value}
        class_set.add(r['class'].value)
        eq_set.add(r['eqid'].value)
    print(f"Found {len(results)} items.")
    print(f"Set is: {class_set}")
    print(f"eq set is: {eq_set}")
    return results, eq_set


def get_vnoms(fdr, eqset):
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
            results[r['id'].value] = float(r['vnomvoltage'].value)

    return results


def get_sensor_config(fdr):
    # get measurement dictionary and set of equipment that is for measurments
    measurements, eqset = get_measurements(fdr)

    # get nominal voltage across the equipment set of id
    vnoms = get_vnoms(fdr, eqset)

    # finally loop over measurements list and add nominal voltageg (normal_value)
    # for the configuration of the measurement.

    for k, v in measurements.items():
        if v['eqid'] in vnoms:
            v['normal_value'] = vnoms[v['eqid']]

    return measurements

if __name__ == '__main__':
    fdr_9500 = "_AAE94E4A-2465-6F5E-37B1-3E72183A4E44"

    pprint(get_sensor_config(fdr_9500))
