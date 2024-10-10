ACTIVITY_NAME: str = "concept:name"
TRACE_ID_NAME: str = "case:concept:name"
from dataclasses import dataclass
# import orjson
import json
import time
from rust_bridge_pm_py import native

@dataclass
class Event:
    attributes: dict[str, str]

    @staticmethod
    def from_activity(activity: str):
        d = dict()
        d[ACTIVITY_NAME] = activity
        event = Event(d)
        return event

    @staticmethod
    def from_dict(d: dict):
        return Event(d.get("attributes", dict()))


    def to_dict(self):
        return {"attributes": self.attributes}

@dataclass
class Trace:
    attributes: dict[str, str]
    events: list[Event]

    @staticmethod
    def from_traceid_events(trace_id: str, events: list[Event]):
        attributes = dict()
        attributes[TRACE_ID_NAME] = trace_id
        trace = Trace(attributes, events)
        return trace

    @staticmethod
    def from_dict(d: dict):
        attributes = d.get("attributes", dict())
        events = [Event.from_dict(t) for t in d.get("events", [])]
        trace = Trace(attributes,events)
        return trace

    def to_dict(self):
        return {
            "events": [t.to_dict() for t in self.events],
            "attributes": self.attributes,
        }

@dataclass
class EventLog:
    attributes: dict[str, str]
    traces: list[Trace]

    @staticmethod
    def from_count(num_traces: int, num_acts: int):
        log = EventLog(dict(), [])
        log.attributes["name"] = "Test EventLog created in Python"
        for i in range(num_traces):
            events = []
            for j in range(num_acts):
                events.append(Event.from_activity("Activity " + str(j)))
            log.traces.append(Trace.from_traceid_events("Trace " + str(i), events))
        return log

    @staticmethod
    def from_dict(d: dict):
        attributes = d.get("attributes", dict())
        traces = [Trace.from_dict(t) for t in d.get("traces", [])]
        log = EventLog(attributes,traces)
        return log

    def to_dict(self):
        return {
            "traces": [t.to_dict() for t in self.traces],
            "attributes": self.attributes,
        }


def py_test_event_log():
    # 200000
    log = create_event_log(200_000, 10)
    total_start = time.time()
    start = time.time()
    # log_dict = log.to_dict()
    # print("Log to dict took " + str((time.time() - start) * 1000) + "ms")
    # start = time.time()
    json_res = json.dumps(log)
    print("JSON dump took " + str((time.time() - start) * 1000) + "ms")
    # new_log_dict = native.test_event_log(log_dict)
    new_log_dict_bytes= native.test_event_log_bytes(json_res)
    print("Native call took " + str((time.time() - start) * 1000) + "ms")
    new_log_dict = json.loads(new_log_dict_bytes)
    start = time.time()
    new_log = EventLog.from_dict(new_log_dict)
    print("Dict to log took " + str((time.time() - start) * 1000) + "ms")
    print("Total: " + str((time.time() - total_start) * 1000) + "ms")
    return new_log


def create_event_log(num_traces: int, num_acts: int):
    return EventLog.from_count(num_traces, num_acts)
