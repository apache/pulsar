import React from "react";
import { ThemeProvider } from "@mui/material/styles";
import theme from "./material-theme";
// import useBaseUrl from "@docusaurus/useBaseUrl";
// import ScriptTag from "../components/ScriptTag";

// Default implementation, that you can customize
function Root({ children }) {
  return (
    <>
      {/* <ScriptTag
        type="text/javascript"
        src="https://buttons.github.io/buttons.js"
      />
      <ScriptTag
        type="text/javascript"
        src="https://cdnjs.cloudflare.com/ajax/libs/clipboard.js/2.0.0/clipboard.min.js"
      />
      <ScriptTag type="text/javascript" src={useBaseUrl("/js/custom.js")} /> */}
      <ThemeProvider theme={theme}>{children}</ThemeProvider>
    </>
  );
}

export default Root;
