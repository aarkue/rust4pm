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
    places: Map<string, { id: string }>;
    transitions: Map<string, { id: string; label: string }>;
    arcs: unknown[];
  },
  OCELShim,
  OCELShim,
];

export type ResultInfoProps =
  | {
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
    };

export type Mode = (typeof MODE_OPTIONS)[number];

export interface WorkerAPI {
  fun: (
    mode: Mode,
    data: Uint8Array,
    isGz: boolean,
    numThreads: number,
  ) => Promise<unknown>;
  init: () => Promise<unknown>;
}
