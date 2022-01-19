import * as React from "react";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableRow from "@mui/material/TableRow";
import Link from "@mui/material/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Translate, { translate } from "@docusaurus/Translate";
import { docUrl, getCache } from "../utils/index";
const versions = require("../../versions.json");

export default function VersionsTable(props) {
  const { siteConfig } = useDocusaurusContext();
  const latestStableVersion = versions[0];

  const repoUrl = `https://github.com/${siteConfig.organizationName}/${siteConfig.projectName}`;
  return (
    <Table size="small" sx={{ maxWidth: 500 }}>
      <TableBody>
        {props.data.map((row) => (
          <TableRow key={row.name}>
            <TableCell
              className="border-gray-300 font-bold"
              sx={{ border: 1, color: "inherit" }}
              align="left"
            >
              <span>{row.name}</span>
            </TableCell>
            <TableCell
              className="border-gray-300"
              sx={{ border: 1 }}
              align="center"
            >
              <Link
                className="text-primary"
                href={docUrl(
                  "",
                  "",
                  row.name == latestStableVersion ? "" : row.name
                )}
                underline="none"
                onClick={() => {
                  getCache().setItem(
                    "version",
                    row.name == "next" ? "master" : row.name
                  );
                }}
              >
                <Translate>Documentation</Translate>
              </Link>
            </TableCell>
            <TableCell
              className="border-gray-300"
              sx={{ border: 1 }}
              align="center"
            >
              <Link
                className="text-primary"
                href={
                  row.name == "next"
                    ? repoUrl
                    : `${siteConfig.baseUrl}release-notes#${row.name}`
                }
                underline="none"
              >
                {row.name == "next"
                  ? translate({
                      message: "Source Code",
                    })
                  : translate({
                      message: "Release Notes",
                    })}
              </Link>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
