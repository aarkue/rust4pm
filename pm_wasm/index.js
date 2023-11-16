import * as Comlink from "comlink";


let go_button = document.getElementById("go-button");
let pre_content = document.getElementById("pre-content");
let checkbox_multithreading = document.getElementById("checkbox-multithread");
go_button.addEventListener("click", () => {
  const workerFun = Comlink.wrap(
    new Worker(new URL("./worker.js", import.meta.url), {
      type: "module",
    })
  );
  go_button.textContent = "Computing...";
  pre_content.textContent = "Computing...\n";
  go_button.disabled = true;
  let input = document.getElementById("log-input");
  input.files[0]
    .text()
    .then(async (xesStr) => {
      const petriNet = await workerFun(xesStr,checkbox_multithreading.checked ? navigator.hardwareConcurrency : 1);
      console.log({ petriNet });
      pre_content.textContent += `Discoverd Petri net:\n ${JSON.stringify(petriNet)}`;
    })
    .finally(() => {
      go_button.textContent = "Go";
      go_button.disabled = false;
    });
});