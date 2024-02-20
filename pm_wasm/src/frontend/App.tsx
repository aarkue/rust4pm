import { ThemeProvider, createTheme, useMediaQuery } from "@mui/material";
import { useMemo } from "react";
import DemoUI from "./DemoUI";

export default function App() {
  const prefersDarkMode = useMediaQuery("(prefers-color-scheme: dark)");
  const theme = useMemo(
    () =>
      createTheme({
        palette: {
          mode: prefersDarkMode ? "dark" : "light",
        },
      }),
    [prefersDarkMode],
  );

  return (
    <ThemeProvider theme={theme}>
      <DemoUI />
    </ThemeProvider>
  );
}
