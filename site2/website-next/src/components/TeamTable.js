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
          {["Name", "Apache Id", "Roles"].map((header) => (
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
              {row.name}
            </TableCell>
            <TableCell
              className="border-gray-300 font-bold"
              sx={{ border: 1, color: "inherit" }}
              align="left"
            >
              {row.apacheId}
            </TableCell>
            <TableCell
              className="border-gray-300 font-bold"
              sx={{ border: 1, color: "inherit" }}
              align="left"
            >
              {row.roles}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
