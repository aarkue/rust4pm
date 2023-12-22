import * as Comlink from "comlink";
import init, {
  initThreadPool,
  wasm_discover_alphappp_petri_net_from_xes_string,
  wasm_discover_alphappp_petri_net_from_xes_vec,
} from "./pkg/pm_wasm.js";

const fun = (data, numThreads) =>
  init().then(async () => {
    await initThreadPool(numThreads);
    console.log("using " + numThreads + "!");
    console.log("Thread pool init", numThreads);
    const start = Date.now();
    let res = "";
    if (typeof data === "string") {
      res = wasm_discover_alphappp_petri_net_from_xes_string(data);
      console.log({ res });
    } else {
      res = wasm_discover_alphappp_petri_net_from_xes_vec(data);
      console.log({ res });
    }
    // let petriNet = wasm_discover_alphappp_petri_net(data);
    console.log(`Call took ${(Date.now() - start) / 1000.0}`);

    return JSON.parse(res);
  });

Comlink.expose(fun);
