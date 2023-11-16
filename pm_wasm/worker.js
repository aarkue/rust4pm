

import * as Comlink from 'comlink';
import init, {  initThreadPool, wasm_discover_alphappp_petri_net } from "./pkg/pm_wasm.js";

const fun = (xesString, numThreads) => init().then(async () => {
  await initThreadPool(numThreads);
  console.log("Thread pool init", numThreads)
      const start = Date.now()
      let petriNet = wasm_discover_alphappp_petri_net(xesString);
      console.log(`Discovery took ${(Date.now() - start) / 1000.0}`);
      return JSON.parse(petriNet);
})


Comlink.expose(fun);
