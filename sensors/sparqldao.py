import math
import types

from gridappsd import GridAPPSD

# Define query prefix
__PREFIXES__ = """
    PREFIX r: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX c: <http://iec.ch/TC57/CIM100#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""


class SparqlDao(object):
    def __init__(self, gappsd: GridAPPSD):

        self._gappsd = gappsd

    def get_energy_consumer_sensor_settings(self, feeder):
        basev = self.get_energy_consumer_basevoltage(feeder, "eqid")
        all_measurements = self.get_all_analog_measurements(feeder, "id")
        sensor_configs = {}

        for k, v in all_measurements.items():
            if v.eqid in basev:
                p = float(basev[v.eqid].p)
                q = float(basev[v.eqid].q)
                nom_mag = math.sqrt(p**2 + q**2)
                nom_ang = math.atan(q/p)
                sensor_configs[v.id] = types.SimpleNamespace(measurement_mird=v.id,
                                                             equipment_mrid=v.eqid,
                                                             normal_magnitude=nom_mag,
                                                             normal_angle=nom_ang,
                                                             name=v.name,
                                                             phase=v.phases)
        return sensor_configs

    def get_all_analog_measurements(self, feeder, key=None):
        query = """
        # list all measurements, with buses and equipments - DistMeasurement
        SELECT ?class ?type ?name ?bus ?cnid ?phases ?eqtype ?eqname ?eqid ?trmid ?id WHERE {
        VALUES ?fdrid {"%s"}
         ?eq c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid. 
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

        return self.__query(__PREFIXES__ + query, key)

    def get_energy_consumer_basevoltage(self, feeder, key=None):
        query = """
            SELECT ?eqid ?name ?bus ?basev ?p ?q ?cnid WHERE {
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
                ?t c:Terminal.ConductingEquipment ?s.
                ?t c:Terminal.ConnectivityNode ?cn. 
                ?cn c:IdentifiedObject.name ?bus.
                ?cn c:IdentifiedObject.mRID ?cnid.
            }
            ORDER by ?name""" % (feeder, )
        return self.__query(query, key)

    def __query(self, query, key=None):
        # print(PREFIX + query)
        results = self._gappsd.query_data(__PREFIXES__ + query)

        message = None
        if key:
            if key not in results['data']['head']['vars']:
                raise ValueError("Invalid key {key} is not in the passed query")
        mykeys = results['data']['head']['vars']

        if key is not None:
            return_values = {}
        else:
            return_values = []

        for row in results['data']['results']['bindings']:

            kv = {}
            for k in mykeys:
                kv[k] = row[k]['value']

            if key is not None:
                return_values[row[key]['value']] = types.SimpleNamespace(**kv)
            else:
                return_values.append(types.SimpleNamespace(**kv))

        return return_values


if __name__ == '__main__':
    feeder = "_AAE94E4A-2465-6F5E-37B1-3E72183A4E44"  # 9500 node
    # feeder = "_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62"  # 13 node
    # feeder = "_6B3B8DF7-4F84-431C-9FA8-68B08E00CD05"  # 123 transactive node

    gappsd = GridAPPSD()
    data = SparqlDao(gappsd)
    cfg = data.get_energy_consumer_sensor_settings(feeder)
    for c, v in cfg.items():
        print(v)
