import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import DownloadIcon from "@mui/icons-material/Download";
import ErrorIcon from "@mui/icons-material/Error";
import {
  Alert,
  Button,
  CircularProgress,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
} from "@mui/material";
import * as Comlink from "comlink";
import { useEffect, useState } from "react";
import { MODE_OPTIONS, Mode, type WorkerAPI } from "./types";
// @ts-ignore: Vite worker URL import
import workerImport from "./worker?worker&url";
import ResultInfo from "./ResultInfo";
let worker: Worker | undefined;
let workerAPI: Comlink.Remote<WorkerAPI>;
export default function DemoUI() {
  const [mode, setMode] = useState<Mode>(MODE_OPTIONS[0]);
  const [workerStatus, setWorkerStatus] = useState<
    "initial" | "ready" | "busy" | "error"
  >("initial");
  const [loading, setLoading] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File>();
  const [result, setResult] = useState<{
    json: unknown;
    durationSeconds: number;
    mode: Mode;
  }>();

  useEffect(() => {
    console.log({ workerImport });
    if (worker === undefined) {
      worker = new Worker(new URL(workerImport, import.meta.url), {
        type: "module",
      });
      // worker = new workerImport();
      workerAPI = Comlink.wrap<WorkerAPI>(worker);
      workerAPI
        .init()
        .then(() => {
          console.log("Init!");
          setWorkerStatus("ready");
        })
        .catch((e) => {
          console.error(e);
          setWorkerStatus("error");
        });
    }
  }, []);

  return (
    <main className="max-w-xl w-full px-6 mx-auto text-center text-black dark:text-white">
      <h1 className="text-7xl font-black mt-4">
        <span className="bg-gradient-to-tr from-orange-400 to-fuchsia-600 bg-clip-text text-transparent">
          WASM
        </span>{" "}
        PoC
      </h1>
      <p>
        <a
          className="text-blue-500 hover:text-blue-600"
          href="https://github.com/aarkue/rust-bridge-process-mining/"
          target="_blank"
        >
          github.com/aarkue/rust-bridge-process-mining/
        </a>
      </p>
      <div className="text-lg my-2">
        {workerStatus === "initial" && (
          <>
            <div>Loading worker...</div>
            <CircularProgress size={16} />
          </>
        )}
        {workerStatus === "ready" && (
          <>
            <div className="">Worker ready</div>
            <CheckCircleIcon color="success" fontSize="small" />
          </>
        )}
        {workerStatus === "error" && (
          <>
            <div className="font-bold">
              Worker error.
              <br />
              See console for details.
            </div>
            <ErrorIcon color="error" fontSize="small" />
          </>
        )}
        {workerStatus === "busy" && "Worker busy..."}
      </div>
      <FormControl sx={{ m: 1, minWidth: 120, width: "100%" }} size="medium">
        <InputLabel id="select-mode-label">Mode</InputLabel>
        <Select
          disabled={loading}
          className="text-left"
          labelId="select-mode-label"
          id="select-mode-select"
          value={mode}
          label="Mode"
          onChange={(v) => {
            setMode(v.target.value as Mode);
          }}
        >
          {MODE_OPTIONS.map((option) => (
            <MenuItem key={option} value={option}>
              {option}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
      <div className="flex flex-col text-left items-start gap-x-2 w-fit mx-auto text-lg mt-2">
        <label htmlFor="input-file" className="font-medium">
          Input file
        </label>
        <input
          onChange={(ev) => {
            if (ev.currentTarget.files && ev.currentTarget.files.length > 0) {
              setSelectedFile(ev.currentTarget.files[0]);
            }
          }}
          type="file"
          id="input-file"
          className="border border-dashed xl:p-6 w-full p-4 rounded"
          accept={
            mode === "Import OCEL2 JSON"
              ? ".json"
              : mode === "Import OCEL2 XML"
              ? ".xml"
              : ".xes,.xes.gz"
          }
        />
      </div>
      <div className="w-fit mx-auto my-2 relative">
        <Button
          size="large"
          variant="contained"
          disabled={loading || selectedFile === undefined}
          onClick={async () => {
            if (!selectedFile) {
              return;
            }
            setLoading(true);
            setWorkerStatus("ready");
            const start = Date.now();
            workerAPI
              .fun(
                mode,
                new Uint8Array(await selectedFile.arrayBuffer()),
                selectedFile.name.endsWith(".gz"),
                1,
              )
              .then((json) => {
                const duration = (Date.now() - start) / 1000.0;
                console.log({ json });
                setResult({ json, durationSeconds: duration, mode });
              })
              .catch((e) => {
                console.error(e);
                setWorkerStatus("error");
                setResult(undefined);
              })
              .finally(() => {
                setLoading(false);
              });
          }}
        >
          Go
        </Button>
        {loading && (
          <CircularProgress
            color="inherit"
            size={24}
            className="absolute left-1/2 top-1/2 -mt-2.5 -ml-2.5"
          />
        )}
      </div>
      {result !== undefined && (
        <Alert
          icon={<CheckCircleIcon fontSize="inherit" />}
          severity="success"
          className="mt-6 text-left relative pb-6"
        >
          <h2 className="font-bold text-xl -mt-1">Success!</h2>
          WASM Call took{" "}
          <span className="font-mono font-semibold">
            {result.durationSeconds}s
          </span>
          <br />
          The result was logged to console and can be downloaded as a .json file
          using the button below.
          <div className="mt-2 font-medium">
            <ResultInfo mode={result.mode} data={result.json as any} />
          </div>
          <div className="absolute right-0 bottom-0">
            <IconButton
              title="Download result"
              onClick={() => {
                const a = document.createElement("a");
                a.download = "result.json";
                const url = URL.createObjectURL(
                  new Blob([JSON.stringify(result.json)], {
                    type: "application/json",
                  }),
                );
                a.href = url;
                a.click();
                URL.revokeObjectURL(url);
                a.remove();
              }}
            >
              <DownloadIcon />
            </IconButton>
          </div>
        </Alert>
      )}
    </main>
  );
}
