from django.http import HttpRequest


def can_subscribe(request: HttpRequest, topic: str) -> bool:
    """The default policy: `public:` topics are open, `user:<pk>` belongs to that user.

    Every other kind of topic is closed. Point EVENTSTREAM_PG["CAN_SUBSCRIBE"] at a function of
    your own for topics such as chat rooms, and fall back to this one for the rest.
    """
    kind, _, key = topic.partition(":")
    match kind:
        case "public":
            return True
        case "user":
            return request.user.is_authenticated and key == str(request.user.pk)
        case _:
            return False
