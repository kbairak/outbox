from outbox import listen


@listen("#")
async def foo(body, routing_key: str):
    print(f"Generic event: {routing_key=}, {body=}")


@listen("user.created", tags={"normal"})
async def on_user_created(body):
    print(f"User created: {body=}")


@listen("user.*", tags={"normal"})
async def on_user_event(body, routing_key: str):
    print(f"User event: {routing_key=}, {body=}")
