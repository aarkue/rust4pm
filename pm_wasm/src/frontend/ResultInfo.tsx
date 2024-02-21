import { ResultInfoProps } from "./types";

export default function ResultInfo({ mode, data }: ResultInfoProps) {
  if (mode === "Import XES & Alpha+++ Discovery") {
    return (
      <div>
        Discovered Petri Net with
        <ul className="[&>li]:list-disc pl-6">
          <li>{Object.keys(data.places).length} Places</li>
          <li>{Object.keys(data.transitions).length} Transitions</li>
          <li>{data.arcs.length} Arcs</li>
        </ul>
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
  console.log({ mode, data });
  return <div></div>;
}
