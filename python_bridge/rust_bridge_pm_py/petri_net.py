import pm4py
from pm4py.objects.petri_net.obj import PetriNet


def dict_to_petrinet(net_dict) -> PetriNet:
    places = {p["id"]: PetriNet.Place(p["id"])
              for p in net_dict["places"].values()}
    transitions = {
        t["id"]: PetriNet.Transition(t["id"], t["label"])
        for t in net_dict["transitions"].values()
    }

    def get_arc_for(arc_dict):
        if arc_dict["from_to"]["type"] == "PlaceTransition":
            fr = places.get(arc_dict["from_to"]["nodes"][0])
            to = transitions.get(arc_dict["from_to"]["nodes"][1])
        else:
            fr = transitions.get(arc_dict["from_to"]["nodes"][0])
            to = places.get(arc_dict["from_to"]["nodes"][1])
        return PetriNet.Arc(fr, to, arc_dict["weight"])

    arcs = [get_arc_for(arc_dict) for arc_dict in net_dict["arcs"]]
    net = PetriNet(None, places.values(), transitions.values(), arcs)
    return net


def petrinet_to_dict(net: PetriNet) -> dict:
    import uuid
    # Used to save a mapping of python ids (e.g., id(p)) to the unique generated uuids (used in the dict)
    pyid_to_uuid = dict()
    places = dict()
    for p in net.places:
        pid = str(uuid.uuid4())
        pyid_to_uuid[id(p)] = pid
        places[pid] = {"id": pid}
    transitions = dict()
    for t in net.transitions:
        tid = str(uuid.uuid4())
        pyid_to_uuid[id(t)] = tid
        transitions[tid] = {"id": tid, "label": t.label}
    arcs = [
        {
            "from_to": {
                "type": "PlaceTransition"
                if type(arc.source) == PetriNet.Place
                else "TransitionPlace",
                "nodes": [pyid_to_uuid[id(arc.source)], pyid_to_uuid[id(arc.target)]],
            },
            "weight": arc.weight,
        }
        for arc in net.arcs
    ]
    return {"places": places, "transitions": transitions, "arcs": arcs}
