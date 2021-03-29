
from dataclasses import asdict, dataclass
import json

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


@dataclass
class ClickEventSanitized(faust.Record):
    timestamp: str
    uri: str
    number: int


app = faust.App("exercise3", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)

#
# : Define an output topic for sanitized click events, without the user email
#
sanitized_topic = app.topic("com.udacity.streams.clickevents.withoutemail",
    key_type=str,
    value_type=ClickEventSanitized,
)
@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for clickevent in clickevents:
        #
        # : Modify the incoming click event to remove the user email.
        #       Create and send a ClickEventSanitized object.
        #
        clickeventsant=ClickEventSanitized(timestamp= clickevent.timestamp,
        uri= clickevent.uri,
        number= clickevent.number)
        #
        # : Send the data to the topic you created above.
        #       Make sure to set a key and value
        #
        await sanitized_topic.send(key=clickeventsant.uri,value=clickeventsant)

if __name__ == "__main__":
    app.main()

