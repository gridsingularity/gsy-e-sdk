Feature: Device Tests

Scenario: API client can connect successfully to a load device and perform all operations
   Given redis container is started
   And d3a container is started using setup file strategy_tests.external_devices
   When the external client is started with test_load_connection
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors
   And the energy bills of the load report the required energy was bought by the load

Scenario: API client can connect successfully to a PV device and perform all operations
   Given redis container is started
   And d3a container is started using setup file strategy_tests.external_devices
   When the external client is started with test_pv_connection
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors
