export interface WorkerAPI {
  fun: (name: "xes-alpha+++"|"ocel2-json"|"ocel2-xml", data: string|Uint8Array, isGz: boolean, numThreads: number) => Promise<string>;
}