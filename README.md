## DistributedKeyValueStorage
### How to run a system.
1. you need to run RemoteRegistry with port number (8005) and registry name as (RemoteRegistry) (This specific values user has to give to test the clients storing & retrieving Data or Node failures using different test cases).
  Expected console out will be: `RMI registry listening on port 8005 Remote registry bound with name 'RemoteRegistry'` 

2. set up the config.properties file. Everything is commented in the config.properties file. The most important properties in the properties file would be buildKVS and nodeid. But also one needs to take care of the WAIT_TIME in ActualKVSNode.

3. After you set up the config.properties file, execute below commands. 
  `ant compile` then `ant kvsnode`
For example, when you want to run a kvsnode as a first actual node in the ring before the Key Value Store system up, you need to setup the file (m=<any m which will generate 2^m node>, buildKVS=y, rmihost=<host name>, rmiport=<rmi port number>, nodeid=<the node id you want, which is nodeid <2^m>).

### More details
More detailed specifications are in the /docs folder. 
This project was implemented as a college assignment. The folder has a documentations named Assignment 3~5_DesignDocument.pdf. The Assignment 5_DesignDocument.pdf describes the finalized version of the RobustDistributedKeyValueSotrage.
