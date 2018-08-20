# Sensor Simulator Application

The sensor simulator application reads from the simulation output topic of the gridappsd message bus, creates random
noise on all points (not including mrid in the name), and then writes those new points to a gridappsd message bus.

## Topics

The following topics will be read and written to.  The <simulation_id> is a GridAPPSD unique identifier for a simulation. 

- reading from: /topic/goss.gridappsd.simulation.output.<simulation_id>
- writing to:  /topic/goss.gridappsd.simulation.sensors.<simulation_id>

## Message Structure

Both the simulation output message and the sensor simulation output will have the same structure see 
(https://gridappsd.readthedocs.io/en/latest/using_gridappsd/index.html#subscribe-to-simulation-output)

