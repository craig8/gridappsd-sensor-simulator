import inspect
import math
import logging
import os
import time

from SPARQLWrapper import SPARQLWrapper2

if os.path.isdir("/gridappsd/services/gridappsd-sensor-simulator"):
    BLAZEGRAPH_URL = "http://blazegraph:8080/bigdata/sparql"
else:
    BLAZEGRAPH_URL = "http://localhost:8889/bigdata/sparql"

CIM100 = "<http://iec.ch/TC57/CIM100#"

# Prefix for all queries.
QUERY_PREFIXES = f"""PREFIX r: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX c: {CIM100}>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""

ROOT_3 = math.sqrt(3)

LOG = logging.getLogger(inspect.getmodulename(__file__))


class Measurements:
    def __init__(self, blazegraph_url=None):
        self._blazegraph_url = BLAZEGRAPH_URL
        if blazegraph_url is not None:
            self._blazegraph_url = blazegraph_url

        self._sparql = SPARQLWrapper2(self._blazegraph_url)

    def get_sensors_meta(self, feeder):
        # get measurement dictionary and set of equipment that is for measurments
        the_measurements = self.get_measurements(feeder)

        nomv_by_cnid = self.get_connected_node_nomv(feeder)
        power_nomv = self.get_fullscale_current(feeder, nomv_by_cnid)
        # # get nominal voltage across the equipment set of id
        # vnoms = get_vnoms(fdr, eqset)
        #
        # # finally loop over measurements list and add nominal voltages (normal_value)
        # # for the configuration of the measurement.
        #
        for k1, v1 in the_measurements.items():
            try:
                v1['cn_nomv'] = nomv_by_cnid[v1['cnid']]
            except KeyError:
                pass
                # print(f"{v}")
            try:
                v1['current_nomv'] = power_nomv[v1['cnid']]
            except KeyError:
                pass
                # print(f"current_nomv key error for {v}")
        return the_measurements

    def get_measurements(self, feeder):
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

        # query = QUERY_PREFIXES + query
        # self._sparql.setQuery(query)
        # ret = self._sparql.query()
        ret = self.__wrap_sparql__(query)
        results = {}

        for r in ret.bindings:
            data = {'class': r['class'].value,
                    'type': r['type'].value,
                    'cnid': r['cnid'].value,
                    "measurement_mrid": r['id'].value}
            results[r['id'].value] = data

        return results

    def get_connected_node_nomv(self, feeder):
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
    """ % (feeder, )

        ret = self.__wrap_sparql__(query)
        results = {}
        for r in ret.bindings:
            results[r['cnid'].value] = float(r['nomv'].value) / ROOT_3
        return results

    def get_fullscale_current(self, feeder: str, cn_nomv: dict):
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
    """ % (feeder,)
        results = {}

        ret = self.__wrap_sparql__(query)
        # query = QUERY_PREFIXES + query
        # sparql.setQuery(query)
        # ret = sparql.query()

        print("Missing Values")
        for r in ret.bindings:
            # print(r)
            try:
                data = dict(eqtype=r['eqtype'].value, eqname=r['eqname'].value,
                            eqid=r['eqid'].value, val=r['val'].value, cn1id=r['cn1id'].value,
                            va_normal=float(r['val'].value) * cn_nomv[r['cn1id'].value])
                results[r['cn1id'].value] = data
            except KeyError as e:
                print(f'{r}')
                # print(e.args)

        return results

    def __wrap_sparql__(self, query_no_prefixes, timeout=10):
        """
        Wrap sparql in a handler for connection errors.  This is especially
        useful when the connection is temporarily interrupted and/or it
        is in a testing environment when the container may not be completely
        up when staring the test.

        :param query_no_prefixes:
        :param timeout:
        :return:
        """
        times_through = 0
        while times_through < timeout:
            try:
                query = QUERY_PREFIXES + query_no_prefixes
                self._sparql.setQuery(query)
                return self._sparql.query()
            except ConnectionResetError:
                times_through += 1

                if times_through > timeout:
                    raise

                time.sleep(1)


#
#

#
# def get_nomv(fdr, eqset):
#     query = """
#     SELECT (xsd:float(?vnom) AS ?vnomvoltage) ?id WHERE {
#         VALUES ?fdrid { "%s" }
#         ?s c:ConnectivityNode.ConnectivityNodeContainer|c:Equipment.EquipmentContainer ?fdr.
#         ?s c:ConductingEquipment.BaseVoltage ?lev.
#         ?s c:IdentifiedObject.mRID ?id .
#         ?lev c:BaseVoltage.nominalVoltage ?vnom.
#     }
# """
#
#     query = QUERY_PREFIXES + query
#     print(query)
#     sparql.setQuery(query)
#     ret = sparql.query()
#     # print(f"VARS are: {ret.variables}")
#     results = {}
#     class_set = set()
#
#     results = {}
#     for r in ret.bindings:
#         if r['id'].value in eqset:
#             #if r['class']== 'Discrete':
#             #    results[r['id'].value] = float(r['vnomvoltage'].value)
#             #elif r['class'] == 'Analog':
#             results[r['id'].value] = float(r['vnomvoltage'].value) / ROOT_3
#             #else:
#             #    raise AttributeError(f"Invalid class found in {r['class']}")
#
#     return results
#


#
#
# if __name__ == '__main__':
#     import json
#     fdr_9500 = "_AAE94E4A-2465-6F5E-37B1-3E72183A4E44"
#     #nomv_cnid, cn1_power = get_sensors_config(fdr_9500)
#
#     my_measurements = get_measurements(fdr_9500)
#     cn_nomv = get_cn_nomv(fdr_9500)
#     current_nom_power = get_fullscale_current(fdr_9500, cn_nomv)
#     print()
#     print("Missing Measurements")
#     for k, v in my_measurements.items():
#         try:
#             v['cn_nomv'] = cn_nomv[v['cnid']]
#         except KeyError:
#             print(f"{v}")
#         # try:
#         #     v['current_nomv'] = current_nom_power[v['cnid']]
#         # except KeyError:
#         #     print(f"current_nomv key error for {v}")
#
#     with open("/home/osboxes/repos/gridappsd-python-log/new_measurement_data.json", 'w') as fp:
#         fp.write(json.dumps(my_measurements, indent=4))
#
#
#     # with open("/home/osboxes/repos/gridappsd-python-log/measurement_first.json") as fp:
#     #     measurement_data = json.load(fp)
#     # #
#     # # lst_meeasurements = get_measurements(fdr_9500)
#     # full = set(measurement_data.keys())
#     # found = set()
#     # for k in measurement_data:
#     #     if k in my_measurements:
#     #         print(f"Found {k}")
#     #         found.add(k)
#     #     else:
#     #         print(f"Not found {k}")
#     #     # if k in nomv_cnid:
#     #     #     print(f"Found {k}")
#     #     # else:
#     #     #     print(f"NOT FOUND {k}")
#     #
#     # print(f"Missing: {full - found}")
#     # print(f"Other missing: {found - full}")
#
#     #pprint(sensors)
