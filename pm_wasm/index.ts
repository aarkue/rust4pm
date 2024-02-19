import * as Comlink from "comlink";
import worker from "./worker?url";
import { WorkerAPI } from "./worker-api";

function hookIntoGoButton() {
  let go_button = document.getElementById("go-button")! as HTMLButtonElement;
  let pre_content = document.getElementById("pre-content")!;
  let checkbox_multithreading = document.getElementById("checkbox-multithread")! as HTMLInputElement;
  go_button.addEventListener("click", async () => {
    let input = document.getElementById("log-input") as HTMLInputElement;
    if (!input.files || input.files.length < 1) {
      pre_content.textContent = "Please select a file.\n";
      return;
    }
    go_button.textContent = "Computing...";
    pre_content.textContent = "Computing...\n";
    go_button.disabled = true;
    const select = document.getElementById("name-select") as HTMLSelectElement;
    let selected_name = select.value === "ocel2-json" ? ("ocel2-json" as const) : ("xes-alpha+++" as const);
    let data =
      selected_name === "ocel2-json" ? await input.files[0].text() : new Uint8Array(await input.files[0].arrayBuffer());
    console.log("Got data");
    const workerFun = Comlink.wrap<WorkerAPI["fun"]>(
      new Worker(new URL(worker, import.meta.url), {
        type: "module",
      })
    );
    console.log("worker:", workerFun);
    if (selected_name === "ocel2-json" && typeof data === "string") {
      // Manual JSON parse for performance comparison
      const start = Date.now();
      const res = JSON.parse(data);
      console.log("JSON.parse res:", res);
      console.log(`JSON.parse call took ${(Date.now() - start) / 1000.0}`);
    }

    const start = Date.now();
    const resJSON = await workerFun(
      selected_name,
      data,
      input.files[0].name.endsWith(".gz"),
      checkbox_multithreading.checked ? 4 : 1
    );
    pre_content.textContent += `Full call took ${(Date.now() - start) / 1000.0}s\nBeginning of Result (see console for full result):\n ${JSON.stringify(resJSON).substring(0,100)}...`;

    go_button.textContent = "Go";
    go_button.disabled = false;
    console.log("----");
  });
}

function main() {
  hookIntoGoButton();
}

main();
