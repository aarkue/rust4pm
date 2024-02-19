import * as Comlink from "comlink";
import { WorkerAPI } from "./worker-api.js";
import init, {
  initThreadPool,
  wasm_discover_alphappp_petri_net_from_xes_string,
  wasm_discover_alphappp_petri_net_from_xes_vec,
  wasm_parse_ocel2_xml,
  wasm_parse_ocel2_json
} from "./pkg/pm_wasm.js";

const fun: WorkerAPI['fun'] = (name: "xes-alpha+++"|"ocel2-json"|"ocel2-xml", data, isGz: boolean, numThreads) =>
  init().then(async () => {
    console.log("Hello from worker");
    await initThreadPool(numThreads);
    console.log("using " + numThreads + "!");
    // console.log("Thread pool init", numThreads);
    const start = Date.now();
    let res = "";
    if(name === "xes-alpha+++"){
      if (typeof data === "string") {
        res = wasm_discover_alphappp_petri_net_from_xes_string(data);
        console.log({ res });
      } else {
        res = wasm_discover_alphappp_petri_net_from_xes_vec(data,isGz);
        console.log({ res });
      }
    }else if(name === "ocel2-json"){
      if(typeof data !== "string"){
        throw new Error("Invalid format: expeceted data as string");
      }
      res = wasm_parse_ocel2_json(data);
      console.log({res});
    
    }else if(name === "ocel2-xml"){
      if(typeof data !== "object"){
        throw new Error("Invalid format: expeceted data as string");
      }
      res = JSON.parse(wasm_parse_ocel2_xml(data));
      console.log({res}); 
    }
    console.log(`Call took ${(Date.now() - start) / 1000.0}`);

    return res;
  });

Comlink.expose(fun);
