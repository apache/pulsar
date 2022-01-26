import * as React from "react";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableRow from "@mui/material/TableRow";
import Link from "@mui/material/Link";
// import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Translate, { translate } from "@docusaurus/Translate";

export default function VersionsTable(props) {
  // const { siteConfig } = useDocusaurusContext();
  return (
    <Table size="small">
      <TableBody>
        <TableRow key="header">
          {["Release", "Binary", "Source", "Release notes"].map((header) => (
            <TableCell
              className="border-gray-300 font-bold"
              sx={{ border: 1, color: "inherit" }}
              align="left"
              key={header}
            >
              <Translate>{header}</Translate>
            </TableCell>
          ))}
        </TableRow>
        {props.data.map((row, index) => (
          <TableRow key={index}>
            <TableCell
              className="border-gray-300 font-bold"
              sx={{ border: 1, color: "inherit" }}
              align="left"
            >
              <Translate>{row.release}</Translate>
            </TableCell>
            <TableCell
              className="border-gray-300"
              sx={{ border: 1 }}
              align="left"
            >
              <Link className="text-primary" href={row.binary} underline="none">
                {row.binaryText + " "}
              </Link>
              (
              <Link
                className="text-primary"
                href={row.binaryAsc}
                underline="none"
              >
                asc
              </Link>
              ,
              <Link
                className="text-primary"
                href={row.binarySha}
                underline="none"
              >
                {row.binaryShaText}
              </Link>
              )
            </TableCell>
            <TableCell
              className="border-gray-300"
              sx={{ border: 1 }}
              align="left"
            >
              <Link className="text-primary" href={row.source} underline="none">
                {row.sourceText + " "}
              </Link>
              (
              <Link
                className="text-primary"
                href={row.sourceAsc}
                underline="none"
              >
                asc
              </Link>
              ,
              <Link
                className="text-primary"
                href={row.sourceSha}
                underline="none"
              >
                {row.sourceShaText}
              </Link>
              )
            </TableCell>
            <TableCell
              className="border-gray-300"
              sx={{ border: 1 }}
              align="left"
            >
              <Link
                className="text-primary"
                href={row.releaseNote}
                underline="none"
              >
                <Translate>Release Notes</Translate>
              </Link>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
