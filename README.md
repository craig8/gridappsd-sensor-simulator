# Sensor Simulator Application

The sensor simulator application reads from the simulation output topic of the gridappsd message bus, creates random
noise on all points (not including mrid in the name), and then writes those new points to a gridappsd message bus.

## Topics

The following topics will be read and written to.  The <simulation_id> is a GridAPPSD unique identifier for a simulation. 

- reading from: /topic/goss.gridappsd.simulation.output.<simulation_id>
- writing to:  /topic/goss.gridappsd.simulation.sensors.<simulation_id>

## Message Structure

Both the simulation output message and teh sensor simulation output will have the same structure see 
(https://gridappsd.readthedocs.io/en/latest/using_gridappsd/index.html#subscribe-to-simulation-output)

## Noise Generation



2)	Add 1% error from a uniform distribution to every numerical value received.  
One way of doing this, using Numpy, would be: 
import numpy as np; val_out = np.random.uniform (0.99 * val_in, 1.01 * val_in)
3)	Publish each val_out on  /topic/goss.gridappsd.simulation.sensors.<simulation_id>
4)	Have an option to drop 1% of the publications from step 3. 
xmit = np.random.uniform (0, 1) and then publish if xmit <= 0.99
