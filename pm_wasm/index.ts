import * as Comlink from "comlink";
import workerURL from "./worker?url";
import { WorkerAPI } from "./worker-api";

function hookIntoGoButton() {
  let go_button = document.getElementById("go-button")! as HTMLButtonElement;
  let pre_content = document.getElementById("pre-content")!;
  let checkbox_multithreading = document.getElementById("checkbox-multithread")! as HTMLInputElement;
  const worker = new Worker(new URL(workerURL, import.meta.url), {
    type: "module",
  });
  const workerAPI = Comlink.wrap<WorkerAPI>(worker);
  go_button.disabled = true;
  workerAPI.init().then(() => {
    console.log("Init!");
    go_button.disabled = false;
  })
  console.log("worker:", workerAPI);
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
    let selected_name =
      select.value === "ocel2-json"
        ? ("ocel2-json" as const)
        : select.value === "ocel2-xml"
        ? ("ocel2-xml" as const)
        : ("xes-alpha+++" as const);
        let data =
        selected_name === "ocel2-json" ? await input.files[0].text() : new Uint8Array(await input.files[0].arrayBuffer());
        console.log("Got data");
        const start = Date.now();
    if (selected_name === "ocel2-json" && typeof data === "string") {
      const res = JSON.parse(data);
      console.log("JSON.parse res:", res);
      console.log(`JSON.parse call took ${(Date.now() - start) / 1000.0}`);
    }
    const startCall = Date.now();
    worker.onmessage = (ev) => {
      pre_content.textContent += `Done!\nFull call took ${
        (Date.now() - start) / 1000.0
      }s\n`;
      console.log({json: ev.data})
  
      go_button.textContent = "Go";
      go_button.disabled = false;
      console.log("----");
      worker.onmessage = null;
    } 
    await workerAPI.fun(
      selected_name,
      data,
      input.files[0].name.endsWith(".gz"),
      checkbox_multithreading.checked ? 4 : 1
    );

  });
}

function main() {
  hookIntoGoButton();
}

main();
