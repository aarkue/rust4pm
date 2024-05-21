export const MODE_OPTIONS = [
  "Import XES & Alpha+++ Discovery",
  "Import OCEL2 JSON",
  "Import OCEL2 XML",
] as const;
type OCELShim = {
  eventTypes: unknown[];
  objectTypes: unknown[];
  events: unknown[];
  objects: unknown[];
};
export type MODE_OPTION_RES = [
  {
    places: Record<string, { id: string }>;
    transitions: Record<string, { id: string; label: string }>;
    arcs: unknown[];
  },
  OCELShim,
  OCELShim,
];

import * as Comlink from "comlink";

export type ResultInfoProps =
  (| {
      mode: (typeof MODE_OPTIONS)[0];
      data: MODE_OPTION_RES[0];
    }
  | {
      mode: (typeof MODE_OPTIONS)[1];
      data: MODE_OPTION_RES[1];
    }
  | {
      mode: (typeof MODE_OPTIONS)[2];
      data: MODE_OPTION_RES[2];
    }) & {workerAPI: Comlink.Remote<WorkerAPI>};

export type Mode = (typeof MODE_OPTIONS)[number];

export interface WorkerAPI {
  fun: (
    mode: Mode,
    data: Uint8Array,
    transferBack: boolean,
    isGz: boolean,
    numThreads: number,
  ) => Promise<unknown>;
  petri_net_to_dot: (json: string) => string;
  init: () => Promise<unknown>;
}
