from SPARQLWrapper import SPARQLWrapper2
import math
import os
import sys
from gridappsd import GridAPPSD
from pprint import pprint

if os.path.isdir("/gridappsd/services/gridappsd-sensor-simulator"):
    blazegraph_url = "http://blazegraph:8080/bigdata/sparql"
else:
    blazegraph_url = "http://localhost:8889/bigdata/sparql"

cim100 = '<http://iec.ch/TC57/CIM100#'
# Prefix for all queries.
PREFIX = """PREFIX r: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX c: {cimURL}>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
""".format(cimURL=cim100)

sparql = SPARQLWrapper2(blazegraph_url)

ROOT_3 = math.sqrt(3)


class SparqlMeasurements(object):
    def __init__(self, gappsd: GridAPPSD, feeder):
        # We need a connection to gridappsd for us to query directly
        # through the interface
        self._gappsd = gappsd
        self._feeder = feeder

    def get_nominal_voltages(self):
        return self._do_query(ENERGY_CONSUMER_NOMV_QUERY % (self._feeder, ),
                              key="eqid",
                              cols=["eqid", "basev", "p", "q"])

    def get_energy_consumer_measurements(self):
        return self._do_query(ENERGY_CONSUMER_MEASUREMENT_QUERY % (self._feeder, ),
                              key="id",
                              cols=["id", "class", "type", "name", "eqid"])

    def get_connectivity_node_nominal_voltages(self):
        results = self._do_query(CONNECTIVITY_NODE_NOMINAL_VOLTAGE_QUERY % (self._feeder, ),
                                key='cnid',
                                cols=['cnid', 'busname', 'nomv'])
        for v in results.values():
            v["nomv"] = float(v["nomv"]) / ROOT_3
        return results

    def _do_query(self, query, key, cols):
        # print(PREFIX + query)
        results = self._gappsd.query_data(PREFIX + query)

        message = None
        if not key in results['data']['head']['vars']:
            message = f"Missing key: {key} in response\n"

        for c in cols:
            if c not in results['data']['head']['vars']:
                if not message:
                    message = ''
                message += f"Missing column: {c} in response\n"

        if message:
            raise SPARQLQueryError(query, message)

        return_values = {}

        for row in results['data']['results']['bindings']:
            d = {}
            for k, v in row.items():
                d[k] = v['value']
            return_values[row[key]['value']] = d
        return return_values




def get_all_measurements(feeder):
    """
    Queries blazegraph for all of the measurements in the passed feeder.

    This function returns a dictionary with the following keys:

        - type (Analog,
    :param feeder:
    :return:
    """
    query = """
        # list all measurements, with buses and equipments - DistMeasurement
        SELECT ?class ?type ?name ?bus ?cnid ?phases ?eqtype ?eqname ?eqid ?trmid ?id WHERE {
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

    query = PREFIX + query
    sparql.setQuery(query)
    ret = sparql.query()
    results = {}

    for r in ret.bindings:
        data = {'type': r['type'].value,
                'cnid': r['cnid'].value,
                'eqtype': r['eqtype'].value,
                'eqid': r['eqid'].value,
                "measurement_mrid": r['id'].value}
        results[r['id'].value] = data

    return results


def get_cn_basevoltage(fdr):
    query = """
    # list all the connectivity node base voltages by feeder, for sensor service
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

    query = PREFIX + query
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


def get_eq_current_limits(feeder: str, cn_nomv: dict):
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
    query = PREFIX + query
    sparql.setQuery(query)
    ret = sparql.query()

    print("Missing Values")
    for r in ret.bindings:
        #print(r)
        try:
            nomv = cn_nomv[r['cn1id'].value]
            data = dict(eqtype=r['eqtype'].value, eqname=r['eqname'].value,
                        eqid=r['eqid'].value, val=r['val'].value, cn1id=r['cn1id'].value,
                        nomv=nomv, normal_value=float(r['val'].value) * nomv)
            results[r['cn1id'].value] = data
        except KeyError as e:
            print(f'{r}')

    return results


def get_nomv(feeder, eqset):
    query = """
    SELECT (xsd:float(?vnom) AS ?vnomvoltage) ?id WHERE {
        VALUES ?fdrid { "%s" }
        ?s c:ConnectivityNode.ConnectivityNodeContainer|c:Equipment.EquipmentContainer ?fdr. 
        ?s c:ConductingEquipment.BaseVoltage ?lev.
        ?s c:IdentifiedObject.mRID ?id . 
        ?lev c:BaseVoltage.nominalVoltage ?vnom.
    }
""" % (feeder, )

    query = PREFIX + query
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


def get_sensor_load_config(feeder):
    consumers = get_energy_consumers_nomv(feeder)
    measurements = get_energy_consumer_measurements(feeder)
    results = {}

    for k, v in measurements.items():
        try:
            results[k] = consumers[v['eqid']]
        except KeyError:
            print(f"Missing consumer for measurement {v}")
    return results


def get_sensors_config(feeder):

    # get measurement dictionary and set of equipment that is for measurments
    all_measurements = get_all_measurements(feeder)

    print(f"There are {len(all_measurements)} measurements.")
    # we only want analog energy consumers.
    energy_consumers = {}
    # types_of_consumers = set()
    for p, v in all_measurements.items():
        row = ""
        for k in v.keys():
            row += f"{k}={v[k]} "
        # print(row)
        # types_of_consumers.add(v['type'])
        if v['eqtype'] == 'EnergyConsumer' and v['type'] != 'Pos':
            energy_consumers[v['measurement_mrid']] = v['cnid']

    for k, v in energy_consumers.items():
        print(f"{k}={v}")

    cn_base_voltages = get_cn_basevoltage(feeder)
    found_set = set()
    for k, v in cn_base_voltages.items():
        for k1, v1 in energy_consumers.items():
            if k == v1:
                found_set.add(v1)
                print(f"Found energy consumer cn node: {k} {all_measurements[k1]['type']}")

    not_found = set()
    for k, v in energy_consumers.items():
        if v in found_set:
            print(f"Not found cn {v}")
    # cn_normal_values = get_eq_current_limits(feeder, cn_base_voltages)

    available_cn_measurements = {}
    missing_measurements = {}

    sys.exit(0)
    # # get nominal voltage across the equipment set of id
    # vnoms = get_vnoms(fdr, eqset)
    #
    # # finally loop over measurements list and add nominal voltageg (normal_value)
    # # for the configuration of the measurement.
    #
    for k1, v1 in all_measurements.items():
        cnid = v1['cnid']
        # try:
        #     v1['cn_nomv'] = cn_base_voltages[cnid]
        # except KeyError:
        #     print(f"Missing nomv_by_cnid[{v1}]")
        try:
            available_cn_measurements[k1] = cn_normal_values[cnid]
            # v1['current_nomv'] = cn_normal_values[cnid]
        except KeyError:
            missing_measurements[cnid] = v1
            #print(f"current_nomv key error for {v1}")

    if missing_measurements:
        with open("missing.measurements.txt", 'w') as fp:
            for k, v in missing_measurements.items():
                fp.write(f"{k} = {v}\n")
    print(f"Found {len(available_cn_measurements)} measurements.")
    return available_cn_measurements


ENERGY_CONSUMER_NOMV_QUERY = PREFIX + """
    # loads (need to account for 2+ unequal EnergyConsumerPhases per EnergyConsumer) - DistLoad
    SELECT ?eqid ?name ?bus ?basev ?p ?q WHERE {
    ?s r:type c:EnergyConsumer.
    # feeder selection options - if all commented out, query matches all feeders
    VALUES ?fdrid {"%s"}
    ?s c:Equipment.EquipmentContainer ?fdr.
    ?fdr c:IdentifiedObject.mRID ?fdrid.
    ?s c:IdentifiedObject.mRID ?eqid.
    ?s c:IdentifiedObject.name ?name.
    ?s c:ConductingEquipment.BaseVoltage ?bv.
    ?bv c:BaseVoltage.nominalVoltage ?basev.
    ?s c:EnergyConsumer.customerCount ?cnt.
    ?s c:EnergyConsumer.p ?p.
    ?s c:EnergyConsumer.q ?q.
    ?s c:EnergyConsumer.phaseConnection ?connraw.
       bind(strafter(str(?connraw),"PhaseShuntConnectionKind.") as ?conn)
    ?s c:EnergyConsumer.LoadResponse ?lr.
    
    OPTIONAL {?ecp c:EnergyConsumerPhase.EnergyConsumer ?s.
    ?ecp c:EnergyConsumerPhase.phase ?phsraw.
       bind(strafter(str(?phsraw),"SinglePhaseKind.") as ?phs) }
    ?t c:Terminal.ConductingEquipment ?s.
    ?t c:Terminal.ConnectivityNode ?cn. 
    ?cn c:IdentifiedObject.name ?bus.
    ?cn c:IdentifiedObject.mRID ?cnid
    }
    GROUP BY ?eqid ?name ?bus ?basev ?p ?q
    ORDER by ?name
"""

ENERGY_CONSUMER_MEASUREMENT_QUERY = """
SELECT ?class ?type ?name ?bus ?phases ?eqtype ?eqname ?eqid ?trmid ?id 
WHERE { 
  ?eq c:Equipment.EquipmentContainer ?fdr. 
  ?fdr c:IdentifiedObject.mRID ?fdrid. 
  VALUES ?fdrid {"%s"} 
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
  {bind(strafter(str(?phsraw),"PhaseCode.") as ?phases)} } 
ORDER BY ?class ?type ?name
"""
# Sent by brandon below, however using the same as what is in gridappsd DistMeasurements query
# is above
# ENERGY_CONSUMER_MEASUREMENT_QUERY = """
# SELECT ?class ?type ?name ?node ?phases ?load ?eqid ?trmid ?id
# WHERE {
#  VALUES ?fdrid {"%s"}
#   ?eq c:Equipment.EquipmentContainer ?fdr.
#   VALUES ?type {"VA"}
#   ?fdr c:IdentifiedObject.mRID ?fdrid.
#   { ?s r:type c:Discrete. bind ("Discrete" as ?class)}
#   UNION
#   { ?s r:type c:Analog. bind ("Analog" as ?class)}
#   ?s c:IdentifiedObject.name ?name .
#   ?s c:IdentifiedObject.mRID ?id .
#   ?s c:Measurement.PowerSystemResource ?eq .
#   ?s c:Measurement.Terminal ?trm .
#   ?s c:Measurement.measurementType ?type .
#   ?trm c:IdentifiedObject.mRID ?trmid.
#   ?eq c:IdentifiedObject.mRID ?eqid.
#   ?eq c:IdentifiedObject.name ?load.
#   ?eq r:type c:EnergyConsumer.
#   ?trm c:Terminal.ConnectivityNode ?cn.
#   ?cn c:IdentifiedObject.name ?node.
#   ?s c:Measurement.phases ?phsraw .
#   {bind(strafter(str(?phsraw),"PhaseCode.") as ?phases)}
# } ORDER BY ?class ?type ?name
# """


CONNECTIVITY_NODE_NOMINAL_VOLTAGE_QUERY = """
# list all the connectivity node base voltages by feeder, for sensor service
SELECT DISTINCT ?busname ?cnid ?nomv WHERE {
# VALUES ?fdrid {"%s"}
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
ORDER by ?busname ?nomv
"""

def get_energy_consumers_nomv(feeder, filter=None):

    query = PREFIX + """
    # loads (need to account for 2+ unequal EnergyConsumerPhases per EnergyConsumer) - DistLoad
    SELECT ?eqid ?name ?bus ?basev ?p ?q WHERE {
    ?s r:type c:EnergyConsumer.
    # feeder selection options - if all commented out, query matches all feeders
    VALUES ?fdrid {"%s"}
    ?s c:Equipment.EquipmentContainer ?fdr.
    ?fdr c:IdentifiedObject.mRID ?fdrid.
    ?s c:IdentifiedObject.mRID ?eqid.
    ?s c:IdentifiedObject.name ?name.
    ?s c:ConductingEquipment.BaseVoltage ?bv.
    ?bv c:BaseVoltage.nominalVoltage ?basev.
    ?s c:EnergyConsumer.customerCount ?cnt.
    ?s c:EnergyConsumer.p ?p.
    ?s c:EnergyConsumer.q ?q.
    ?s c:EnergyConsumer.phaseConnection ?connraw.
       bind(strafter(str(?connraw),"PhaseShuntConnectionKind.") as ?conn)
    ?s c:EnergyConsumer.LoadResponse ?lr.
    
    OPTIONAL {?ecp c:EnergyConsumerPhase.EnergyConsumer ?s.
    ?ecp c:EnergyConsumerPhase.phase ?phsraw.
       bind(strafter(str(?phsraw),"SinglePhaseKind.") as ?phs) }
    ?t c:Terminal.ConductingEquipment ?s.
    ?t c:Terminal.ConnectivityNode ?cn. 
    ?cn c:IdentifiedObject.name ?bus.
    ?cn c:IdentifiedObject.mRID ?cnid
    }
    GROUP BY ?eqid ?name ?bus ?basev ?p ?q
    ORDER by ?name
""" % (feeder, )

    print(query)
    sparql.setQuery(query)
    ret = sparql.query()

    results = {}
    for r in ret.bindings:
        p = float(r['p'].value)
        q = float(r['q'].value)
        results[r['eqid'].value] = dict(power=math.sqrt(p ** 2 + q ** 2),
                                        p=p,
                                        q=q,
                                        eqid=r['eqid'].value,
                                        name=r['name'].value,
                                        bus=r['bus'].value,
                                        basev=r['basev'].value)

    return results


def get_energy_consumer_measurements(feeder):
    query = PREFIX + """   
SELECT ?class ?type ?name ?node ?phases ?load ?eqid ?trmid ?id 
WHERE { 
 VALUES ?fdrid {"%s"} 
  ?eq c:Equipment.EquipmentContainer ?fdr. 
  VALUES ?type {"VA"}
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
  ?eq c:IdentifiedObject.name ?load. 
  ?eq r:type c:EnergyConsumer. 
  ?trm c:Terminal.ConnectivityNode ?cn. 
  ?cn c:IdentifiedObject.name ?node. 
  ?s c:Measurement.phases ?phsraw . 
  {bind(strafter(str(?phsraw),"PhaseCode.") as ?phases)} 
} ORDER BY ?eqid""" % (feeder, )
    sparql.setQuery(query)
    ret = sparql.query()

    results = {}
    for r in ret.bindings:
        results[r['id'].value] = dict(
            class_name=r['class'].value,
            type_name=r['type'].value,
            name=r['name'].value,
            eqid=r['eqid'].value)
        # if r['srid'].value in filter:
        #     p = r['p'].value
        #     q = r['q'].value
        #     results[r['srid'].value] = math.sqrt(p ** 2 + q ** 2)
        #     # else:
        #     #    raise AttributeError(f"Invalid class found in {r['class']}")

    return results


def get_measurements_brandon(feeder):
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
         ?eq r:type c:EnergyConsumer.
         ?eq r:type ?typeraw.
          bind(strafter(str(?typeraw),"#") as ?eqtype)
         ?trm c:Terminal.ConnectivityNode ?cn.
         ?cn c:IdentifiedObject.name ?bus.
          ?cn c:IdentifiedObject.mRID ?cnid.
         ?s c:Measurement.phases ?phsraw .
           {bind(strafter(str(?phsraw),"PhaseCode.") as ?phases)}
} ORDER BY ?class ?type ?name
""" % (feeder, )

    query = PREFIX + query
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


def get_nominal_values(feeder):
    """
    Return a dictionary with a key being the measurement mrid and the value being the nominal value of
    the measurement.

    :param feeder:
    :return:
    """

    the_measurements = get_all_measurements(feeder)
    cn_nomv = get_cn_basevoltage(feeder)
    full_scale_values = get_eq_current_limits(feeder, cn_nomv)

    for k, v in the_measurements.items():
        for k1, v1 in full_scale_values.items():
            if v['eqid'] == v1['eqid']:
                print("found eqid!")
        # if v['cnid'] in cn_nomv:
        #     for k1, v1 in full_scale_values.items():
        #
        #     print(f"{k} found in cn_nomv")
        # if k in full_scale_values:
        #     print("Found it")
        # else:
        #     print("not found it")
    # for k, v in full_scale_values.items():
    #     if k in cn_nomv:
    #         print("Found k in cn_nomv")
    #     else:
    #         print("Not found k in cn_nomv")
    return

    for k, v in the_measurements.items():

        cn_id = v['cnid']
        if cn_id in cn_nomv:
            print(f"connectivity node measurement {k}: bus={v['cnid']}, {v['type']}, {v['eqtype']}")

        if cn_id in full_scale_values.keys():
            print(f"in full scale stuff: {cn_id}")

        if k in full_scale_values:
            print(f"key is: {k}")

        # try:
        #
        #     value = full_scale_values[v['cnid']]
        #     print(f"Found value {value}")
        # except KeyError:
        #     pass
        # if v['cnid'] in full_scale_values:
        #     print(f"fullscale {v['cnid']}")

        # if k in cn_nomv[v['cnid']]:
        #     print(f"{k} is in cn_nomv")
        #
        # if k in full_scale_values:
        #     print(f"{k} is in full_scale")

    # merged_results = {}
    # for k, v in full_scale_values.items():
    #
    #     print(f"{v['eqtype']}, {v['cn1id']}, {cn_nomv[v['cn1id']]}")
    #     # print(v['cn1id'])


if __name__ == '__main__':
    import json
    fdr_9500 = "_AAE94E4A-2465-6F5E-37B1-3E72183A4E44"
    fdr_13 = "_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62"

    feeder = fdr_13

    measurements = get_all_measurements(feeder)

    sensor_config = get_sensors_config(feeder)
    #
    # # This is using the "load only" configuration for getting the nominal voltage.
    # #sensor_config = get_sensor_load_config(feeder)
    #
    # with open("sensor.measurements.txt", 'w') as fp:
    #     for k, v in sensor_config.items():
    #         fp.write(f"{k} = {v}\n")
    #pprint(sensor_config)
    # sensors = get_sensors_config(fdr_13, "TOM")
    # #sensors = get_sensors_config(fdr_9500)
    # for s, v in sensors.items():
    #     if 'current_nomv' in v:
    #         print("Got one!")
    # get_nominal_values(fdr_9500)
    # with open("nominal_v.txt", 'w') as fp:
    #     json.dump(get_nominal_values(fdr_9500), fp, indent=4)

    #tom_measurements = get_measurements(fdr_9500, "TOM")
    #brandon_measurements = get_measurements(fdr_9500, "BRANDON")


    #nomv_cnid, cn1_power = get_sensors_config(fdr_9500)

    # my_measurements = get_measurements(fdr_9500)
    # cn_nomv = get_cn_nomv(fdr_9500)
    # current_nom_power = get_fullscale_current(fdr_9500, cn_nomv)
    # print()
    # print("Missing Measurements")
    # for k, v in my_measurements.items():
    #     try:
    #         v['cn_nomv'] = cn_nomv[v['cnid']]
    #     except KeyError:
    #         print(f"{v}")
    #     # try:
    #     #     v['current_nomv'] = current_nom_power[v['cnid']]
    #     # except KeyError:
    #     #     print(f"current_nomv key error for {v}")
#    get_energy_consumers_nomv
#    with open("/home/osboxes/repos/gridappsd-python-log/new_measurement_data.json", 'w') as fp:
#        fp.write(json.dumps(my_measurements, indent=4))


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

class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class SPARQLQueryReturnEmptyError(Error):
    """Raised if a SPARQL query returns empty bindings.

    Attributes:
        query -- SPARQL query that resulted in empty return.
        message -- explanation of the error.
    """

    def __init__(self, query, message):
        self.query = query.replace('\n', '\n    ')
        self.message = message


class SPARQLQueryError(Error):
    """Raised if a SPARQL query returns an error.

    Attributes:
        query -- SPARQL query that resulted in error.
        message -- explanation of the error.
    """

    def __init__(self, query, message):
        self.query = query.replace('\n', '\n    ')
        self.message = message
