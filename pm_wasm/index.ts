import * as Comlink from "comlink";
import worker from "./worker?url"

function hookIntoGoButton() {
  let go_button = document.getElementById("go-button")! as HTMLButtonElement;
  let pre_content = document.getElementById("pre-content")!;
  let checkbox_multithreading = document.getElementById("checkbox-multithread")! as HTMLInputElement;
  go_button.addEventListener("click", async () => {
    let input = document.getElementById("log-input") as HTMLInputElement;
    if(!input.files || input.files.length < 1){
      pre_content.textContent = "Please select a file.\n";
      return;
    }
    go_button.textContent = "Computing...";
    pre_content.textContent = "Computing...\n";
    go_button.disabled = true;
    let data = new Uint8Array(await input.files[0].arrayBuffer());
    console.log("Got data");
      const workerFun = Comlink.wrap(
          new Worker(new URL(worker, import.meta.url), {
            type: "module",
          })
        );
    const petriNet = await workerFun(data,checkbox_multithreading.checked ? 4 : 1);
    pre_content.textContent += `Discoverd Petri net:\n ${JSON.stringify(petriNet)}`;
    go_button.textContent = "Go";
    go_button.disabled = false;
    console.log("----")
    
    // Below: Use string instead

    // input
    //   .files![0].text()
    //   .then(async (xesStr) => {
    //     const workerFun = Comlink.wrap(
    //       new Worker(new URL(worker, import.meta.url), {
    //         type: "module",
    //       })
    //     );
    //     const petriNet = await workerFun(xesStr,checkbox_multithreading.checked ? 2 : 1);
    //     console.log({ petriNet });
    //     pre_content.textContent += `Discoverd Petri net:\n ${JSON.stringify(petriNet)}`;
    //   })
    //   .finally(() => {
    //   });
  });
}

function main() {
    hookIntoGoButton()
}

main();
