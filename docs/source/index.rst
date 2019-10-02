.. GridAPPSD-Python documentation master file, created by
   sphinx-quickstart on Wed Aug  7 17:08:44 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

GridAPPSD's Sensor Simulator
============================

.. toctree::
   :maxdepth: 3

The `GridAPPSD's Sensor Simulator` simulates real devices based upon the magnitude of "prestine" simulated values.  This
service has been specifically designed to work within the gridappsd platform container.  The `GridAPPSD` platform will
start the service when it is specified as a dependency of an application or when a service configuration is specified
within the `GridAPPSD Visualization <https://gridappsd.readthedocs.io/en/latest/using_gridappsd/index.html>`_.  The image
below shows a portion of the configuration options available through the service configuration panel.

.. image:: /_static/sensor-simulator-service-configuration.png

Service Configuration
---------------------

The sensor-config in the above image shows an example of how to configure a portion of the system to have sensor output.
Each mrid (`_99db0dc7-ccda-4ed5-a772-a7db362e9818`) will be monitored by this service and either use the default values
or use the specified values during the service runtime.

.. code-block:: json

   {
      "_99db0dc7-ccda-4ed5-a772-a7db362e9818": {
         "nominal-voltage": 100,
         "perunit-confidence-rate": 0.95,
         "aggregation-interval": 30,
         "perunit-drop-rate": 0.01
      },
      "_ee65ee31-a900-4f98-bf57-e752be924c4d":{},
      "_f2673c22-654b-452a-8297-45dae11b1e14": {}
   }

The other options for the service are:

 * default-perunit-confidence-rate
 * default-aggregation-interval
 * default-perunit-drop-rate
 * passthrough-if-not-specified

These options will be used when not specified within the sensor-config block.

.. note::

   Currently the nominal-voltage is not looked up from the database.  At this time services aren't able to tell
   the platform when they are "ready".  This will be implemented in the near future and then all of the nominal-voltages
   will be queried from the database.

Further information about the `GridAPPSD <https://gridappsd.readthedocs.org/>`_ platform can be found at
`https://gridappsd.readthedocs.org <https://gridappsd.readthedocs.org>`_.



