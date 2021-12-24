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
          {["Release", "Link", "Crypto files"].map((header) => (
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
          <TableRow key={row.release}>
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
              <Link className="text-primary" href={row.link} underline="none">
                {row.linkText}
              </Link>
            </TableCell>
            <TableCell
              className="border-gray-300"
              sx={{ border: 1 }}
              align="left"
            >
              <Link className="text-primary" href={row.asc} underline="none">
                asc
              </Link>
              ,
              <Link className="text-primary" href={row.sha512} underline="none">
                sha512
              </Link>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
