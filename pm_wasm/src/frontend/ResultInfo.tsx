import { useEffect, useState } from "react";
import { ResultInfoProps } from "./types";

export default function ResultInfo({ mode, data, workerAPI }: ResultInfoProps) {
  if (mode === "Import XES & Alpha+++ Discovery") {
    return (
      <div>
        Discovered Petri Net with
        <ul className="[&>li]:list-disc pl-6">
          <li>{Object.keys(data.places).length} Places</li>
          <li>{Object.keys(data.transitions).length} Transitions</li>
          <li>{data.arcs.length} Arcs</li>
        </ul>
        <PetriNetRenderer petriNetJSON={JSON.stringify(data)} workerAPI={workerAPI} />
      </div>
    );
  }
  if (mode === "Import OCEL2 JSON" || mode === "Import OCEL2 XML") {
    return (
      <div>
        Imported OCEL2.0 with
        <ul className="[&>li]:list-disc pl-6">
          <li>{data.objectTypes.length} Object Types</li>
          <li>{data.eventTypes.length} Event Types</li>
          <li>{data.objects.length} Objects</li>
          <li>{data.events.length} Events</li>
        </ul>
      </div>
    );
  }
  return <div></div>;
}

import { Graphviz } from "@hpcc-js/wasm/graphviz";

function PetriNetRenderer({
  petriNetJSON,
  workerAPI,
}: {
  petriNetJSON: string;
  workerAPI: ResultInfoProps["workerAPI"];
}) {
  const [graphviz, setGraphviz] = useState<Graphviz>();
  const [svg, setSVG] = useState<string>("");
  useEffect(() => {
    if (graphviz) {
      workerAPI.petri_net_to_dot(petriNetJSON).then((dot) => {
        setSVG(graphviz.dot(dot));
      });
    }
  }, [graphviz, petriNetJSON, workerAPI]);

  useEffect(() => {
    Graphviz.load().then((gv) => setGraphviz(gv));
  }, []);

  return <div className="flex mt-0 [&>svg]:h-fit [&>svg]:w-fit p-1 m-2 rounded bg-white mb-8" dangerouslySetInnerHTML={{__html: svg}}></div>;
}
