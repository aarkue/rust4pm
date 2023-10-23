from typing import Optional, Tuple
import pm4py
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.petri_net.utils.petri_utils import add_arc_from_to

def dict_to_petrinet(
    net_dict,
) -> Tuple[PetriNet, Optional[Marking], Optional[Marking]]:
    places = {p["id"]: PetriNet.Place(p["id"]) for p in net_dict["places"].values()}
    transitions = {
        t["id"]: PetriNet.Transition(t["id"], t["label"])
        for t in net_dict["transitions"].values()
    }

    net = PetriNet(None, places.values(), transitions.values())
    def get_arc_for(arc_dict):
        if arc_dict["from_to"]["type"] == "PlaceTransition":
            fr = places.get(arc_dict["from_to"]["nodes"][0])
            to = transitions.get(arc_dict["from_to"]["nodes"][1])
        else:
            fr = transitions.get(arc_dict["from_to"]["nodes"][0])
            to = places.get(arc_dict["from_to"]["nodes"][1])
        return add_arc_from_to(fr,to,net,arc_dict["weight"])

    arcs = [get_arc_for(arc_dict) for arc_dict in net_dict["arcs"]]

    # Initial and Final Markings
    im = None
    if net_dict["initial_marking"] is not None:
        im = Marking()
        for place_id in net_dict["initial_marking"]:
            im[places[place_id]] = net_dict["initial_marking"][place_id]
    fm = None
    if net_dict["final_markings"] is not None:
        if len(net_dict["final_markings"]) > 0:
            if len(net_dict["final_markings"]) > 1:
                print(
                    "Warning: PetriNet contains more than one final marking. For compability only the first final marking will be considered. This might not be the intended outcome!"
                )
        fm = Marking()
        for place_id in net_dict["final_markings"][0]:
            fm[places[place_id]] = net_dict["final_markings"][0][place_id]

    return (net, im, fm)


def petrinet_to_dict(
    net: PetriNet, im: Optional[Marking] = None, fms: Optional[list[Marking]] = None
) -> dict:
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
    initial_marking = None
    if im is not None:
        initial_marking = dict()
        for p, n in im.items():
            initial_marking[pyid_to_uuid[id(p)]] = n

    final_markings = None
    if fms is not None:
        final_markings = list()
        for fm in fms:
            final_marking = dict()
            for p, n in fm.items():
                final_marking[pyid_to_uuid[id(p)]] = n
            final_markings.append(final_marking)

    return {
        "places": places,
        "transitions": transitions,
        "arcs": arcs,
        "initial_marking": initial_marking,
        "final_markings": final_markings,
    }
