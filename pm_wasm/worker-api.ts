export interface WorkerAPI {
  fun: (name: "xes-alpha+++"|"ocel2-json", data: string|Uint8Array, isGz: boolean, numThreads: number) => Promise<string>;
}