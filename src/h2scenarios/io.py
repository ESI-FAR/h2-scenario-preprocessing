"""Data cleanup utilities."""

from collections import defaultdict
from pathlib import Path
import sqlite3

import duckdb
import pandas as pd
import parse
import xarray as xr


def csv_to_dfs(dirname: str):
    """Read CSVs from dir tree, and convert to `pandas.DataFrame`."""
    template = "{which_data}_{technology:l}_{distance:d}_{bound_eco:l}_{bound_tech:l}_{pipe_kind:l}_pipe.csv"
    dst = Path(dirname)
    csvs = list(dst.glob("**/*.csv"))
    dfs = defaultdict(list)

    for c in csvs:
        parse_result = parse.parse(template, c.name)
        which_data = parse_result.named["which_data"]
        # this key is not needed in the dataframes themselves
        del parse_result.named["which_data"]

        topdir = c.relative_to(dirname).parts[0]  # D-OFF, C-{OFF,ON}
        df = pd.read_csv(c, index_col=0).assign(sysconfig=topdir, **parse_result.named)

        # prepared for having the 'sistem' typo be fixed
        if which_data in ["sistem costs", "system costs"]:
            # Use component name as part of index
            columns_to_index = df.columns[-6:].values.tolist() + [df.columns[0]]

        elif which_data == "simp_output_results":
            # ignore index, only has a single entry
            columns_to_index = df.columns[-6:].values.tolist()

        elif which_data == "time_dep_costs":
            # use index as year count
            df["year"] = df.index.values
            columns_to_index = df.columns[-7:].values.tolist()
        else:
            raise RuntimeError(f"{c}: could not match {which_data=}")

        df["rep_pipe"] = df.pop("pipe_kind") == "rep"
        columns_to_index = [
            "rep_pipe" if c == "pipe_kind" else c for c in columns_to_index
        ]
        df = df.set_index(columns_to_index)
        dfs[which_data].append(df)

    # Merge them into one DataFrame per 'which_data'
    combined_dfs = {}
    for which_data, organized in dfs.items():
        _df = pd.concat(organized)
        # '/' is not accepted as a variable name by NetCDF, rename to '_per_'
        _df.columns = [col.replace("/", "_per_") for col in _df.columns]
        combined_dfs[which_data] = _df

    return combined_dfs


def multi_indexed_dfs_to_xarray(multi_indexed_dfs):
    """Merge dataframes w/ a multiindex into a dataset.

    XArray merge each multi-indexed dataframe into a single Dataset:
    - the coordinates (multi-index) match between files
    - data columns become DataArrays

    :param multi_indexed_dfs:
    :return:
    """
    ds = xr.merge(xr.Dataset.from_dataframe(df) for df in multi_indexed_dfs.values())
    return ds


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(__doc__)
    parser.add_argument("input_dir")
    parser.add_argument("-o", "--output", default="", help="Output NetCDF file")
    parser.add_argument(
        "-t",
        "--type",
        choices=["duckdb", "sqlite", "netcdf"],
        default="netcdf",
        help="Output format",
    )
    opts = parser.parse_args()

    multi_indexed_dfs = csv_to_dfs(opts.input_dir)

    ds = multi_indexed_dfs_to_xarray(multi_indexed_dfs)
    df = ds.to_dataframe()

    if opts.output:
        outfile = opts.output
    else:
        opts.type = "netcdf"
        outdir = opts.input_dir.rsplit("/", maxsplit=1)[0]
        outfile = f"{outdir}/data-Laurens-HyChain_WP1.nc"

    match opts.type:
        case "netcdf":
            print(f"Writing to NetCDF: {outfile}")
            ds.to_netcdf(outfile)
        case "sqlite":
            print(f"Writing to SQLite: {outfile}, table 'h2_scenarios'")
            con = sqlite3.connect(opts.output)
            df.to_sql("h2_scenarios", con)
            con.close()
        case "duckdb":
            print(f"Writing to DuckDB: {outfile}, table 'h2_scenarios'")
            con = duckdb.connect(opts.output)
            con.register("df", df.reset_index())
            con.sql("CREATE OR REPLACE TABLE h2_schenarios AS SELECT * FROM df")
            con.close()
