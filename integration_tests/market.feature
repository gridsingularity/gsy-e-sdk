Feature: Market Tests

Scenario: API client can connect successfully to a market and perform all operations
  Given redis container is started
  And gsy-e is started using setup strategy_tests.external_market_stats (-t 60s -s 60m -d 4h --slot-length-realtime 2s)
  And the external client is started that connects to house-2 market
  Then the external client does not report errors

