Feature: Market Tests

Scenario: DSO market stats endpoint is functional
  Given redis container is started
  And d3a is started using setup strategy_tests.external_market_stats (-t 1s -s 60m)
  And DSO started the external client that connects to House 2 market
  When the external client requests DSO market stats
  Then the external client does not report errors

Scenario: Changing constant grid fee is functional
  Given redis container is started
  And d3a is started using setup strategy_tests.external_market_stats (-t 1s -s 60m)
  And DSO started the external client that connects to House 2 market
  Then the external client is connecting to the simulation until finished
  And the external client does not report errors
