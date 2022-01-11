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
          {["Name", "Scope", "", "", ""].map((header) => (
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
              {row.email}
            </TableCell>
            <TableCell
              className="border-gray-300 font-bold"
              sx={{ border: 1, color: "inherit" }}
              align="left"
            >
              {row.desc}
            </TableCell>
            <TableCell
              className="border-gray-300"
              sx={{ border: 1, color: "inherit" }}
              align="left"
            >
              <Link
                className="text-primary"
                href={row.subscribe}
                underline="none"
                target="_blank"
              >
                <Translate>Subscribe</Translate>
              </Link>
            </TableCell>
            <TableCell
              className="border-gray-300"
              sx={{ border: 1, color: "inherit" }}
              align="left"
            >
              <Link
                className="text-primary"
                href={row.unsubscribe}
                underline="none"
                target="_blank"
              >
                <Translate>Unsubscribe</Translate>
              </Link>
            </TableCell>
            <TableCell
              className="border-gray-300"
              sx={{ border: 1, color: "inherit" }}
              align="left"
            >
              <Link
                className="text-primary"
                href={row.archives}
                underline="none"
                target="_blank"
              >
                <Translate>Archives</Translate>
              </Link>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
