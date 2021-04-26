from behave import given, when, then

@then("all events and commands where send to on_event_and_response")
def step_impl(context):
    assert context.device.events_or_responses == \
           {"batch_commands", "tick", "trade", "market", "finish"}
