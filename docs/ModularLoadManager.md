# Modular Load Manager

The modular load manager, implemented in `ModularLoadManagerImpl`, is a flexible alternative to the previously
implemented load manager `SimpleLoadManagerImpl` which attempts to simplify how load is manager while also providing
abstractions so that complex load management strategies may be implemented.

## Usage

To use the modular load manager, change

`loadManagerClassName=com.yahoo.broker.loadbalance.impl.SimpleLoadManagerImpl`

in `broker.conf` to

`loadManagerClassName=com.yahoo.broker.loadbalance.impl.ModularLoadManagerImpl`

Alternatively, the load manager may also be changed dynamically via the `pulsar-admin` tool as follows:

`pulsar-admin update-dynamic-config --config loadManagerClassName --value 
com.yahoo.broker.loadbalance.impl.ModularLoadManagerImpl`

The admin tool may also be used to change back to `com.yahoo.broker.loadbalance.impl.SimpleLoadManagerImpl`
Note that the ZooKeeper node `/admin/configuration` must exist BEFORE brokers are started for this to work correctly.