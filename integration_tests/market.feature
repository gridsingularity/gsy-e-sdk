Feature: Market Tests

Scenario: Market stats endpoint is functional
  Given redis container is started
  And d3a container is started using setup file strategy_tests.external_market_stats
  And the external client is started that connects to house-2 market
#  When the external client is requesting market stats
#  Then the market stats are reported correctly