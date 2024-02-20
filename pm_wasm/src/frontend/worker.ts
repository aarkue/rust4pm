import * as Comlink from "comlink";
import init, {
  wasm_discover_alphappp_petri_net_from_xes_string,
  wasm_discover_alphappp_petri_net_from_xes_vec,
  wasm_parse_ocel2_json,
  wasm_parse_ocel2_xml_to_json_vec,
} from "../../pkg/pm_wasm.js";
import type { WorkerAPI } from "./types.js";

const fun: WorkerAPI["fun"] = async (mode, data, isGz: boolean) => {
  console.log("Hello from worker!");
  const start = Date.now();
  let res;

  console.time("Pure WASM Call");
  if (mode === "Import XES & Alpha+++ Discovery") {
    if (typeof data === "string") {
      res = wasm_discover_alphappp_petri_net_from_xes_string(data);
    } else {
      res = wasm_discover_alphappp_petri_net_from_xes_vec(data, isGz);
    }
  } else if (mode === "Import OCEL2 JSON") {
    res = wasm_parse_ocel2_json(data);
  } else if (mode === "Import OCEL2 XML") {
    res = JSON.parse(
      new TextDecoder().decode(wasm_parse_ocel2_xml_to_json_vec(data)),
    );
    // res = JSON.parse(wasm_parse_ocel2_xml_to_json_str(data));
    // res = wasm_parse_ocel2_xml(data);
  }
  console.timeEnd("Pure WASM Call");
  console.log(`Call took ${(Date.now() - start) / 1000.0}`);
  return res;
};

Comlink.expose({
  fun,
  init: async () => {
    await init();
  },
} satisfies WorkerAPI);
