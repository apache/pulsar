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
          {props.header.map((header) => (
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
            {Object.entries(row).map(([key, val]) => {
              return (
                <TableCell
                  key={key}
                  className="border-gray-300"
                  sx={{ border: 1, color: "inherit" }}
                  align="left"
                >
                  {(() => {
                    if (typeof val == "object") {
                      if (val.constructor == Array) {
                        return val.map((e) => {
                          return (
                            <div>
                              <Link
                                className="text-primary"
                                href={e.href}
                                underline="none"
                                target="_blank"
                              >
                                {e.text}
                              </Link>
                            </div>
                          );
                        });
                      } else {
                        return (
                          <Link
                            className="text-primary"
                            href={val.href}
                            underline="none"
                            target="_blank"
                          >
                            {val.text}
                          </Link>
                        );
                      }
                    } else {
                      return val;
                    }
                  })()}
                </TableCell>
              );
            })}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
