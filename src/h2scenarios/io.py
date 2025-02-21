"""Data cleanup utilities."""

from collections import defaultdict
from pathlib import Path
import sqlite3
from typing import cast

import duckdb
import pandas as pd
import parse
import xarray as xr


_trans_table = str.maketrans(
    {"[": "_", "]": None, " ": None, "€": "Euro", "/": "_per_", "%": "percent"}
)

_component_grps = {
    "WT": "Wind farm",
    "RO_unit": "Water desalination and brine disposal",
    "brine_disp": "Water desalination and brine disposal",
    "ALK": "Electrolyzer",
    "PEM": "Electrolyzer",
    "H2_On_repur_pipelines": "Hydrogen infrastructure",
    "H2_off_repur_pipelines": "Hydrogen infrastructure",
    "H2_off_new_pipelines": "Hydrogen infrastructure",
    "H2_On_new_pipelines": "Hydrogen infrastructure",
    "H2_local_pipelines": "Hydrogen infrastructure",
    "H2_substation": "Offshore hydrogen platform",
    "Compressor_on": "Compressor",
    "Compressor_off": "Compressor",
    "grid_energy": "Compressor",
    "AC_grid_cables": "Compressor",
    "IA_cables": "Electrical infrastructure",
    "AC_cables": "Electrical infrastructure",
    "AC_electric_substructure": "Electrical infrastructure",
    "DC_cables": "Electrical infrastructure",
    "DC_electric_substructure": "Electrical infrastructure",
    "H2_onshore": "",
}


def csv_to_dfs(dirname: str):
    """Read CSVs from dir tree, and convert to `pandas.DataFrame`."""
    template = (
        "{which_data}_{technology:l}_{distance:d}"
        "_{bound_eco:l}_{bound_tech:l}_{pipe_kind:l}_pipe.csv"
    )
    dst = Path(dirname)
    csvs = list(dst.glob("**/*.csv"))
    dfs = defaultdict(list)

    for path in csvs:
        parse_result = cast(parse.Result, parse.parse(template, path.name))
        which_data = parse_result["which_data"]
        # this key is not needed in the dataframes themselves
        columns = {k: v for k, v in parse_result.named.items() if k != "which_data"}

        topdir = path.relative_to(dirname).parts[0]  # D-OFF, C-{OFF,ON}
        df = pd.read_csv(path, index_col=0).assign(sysconfig=topdir, **columns)

        match which_data:
            case "sistem costs" | "system costs":
                which_data = "system_costs"
                # "component[-]" -> "component"
                df.columns = [
                    "component" if "component" in c else c for c in df.columns
                ]
                df = df.assign(component_groups=df["component"].map(_component_grps))
            case "time_dep_costs":
                df["year"] = df.index.values
            case "simp_output_results":
                pass
            case _:
                raise RuntimeError(f"{path}: could not match {which_data=}")

        df["rep_pipe"] = df.pop("pipe_kind") == "rep"

        idx_cols = [
            col for col, type_ in df.dtypes.items() if type_ != float  # noqa: E721
        ]
        df = df.set_index(idx_cols)
        dfs[which_data].append(df)

    combined_dfs = {}
    for which_data, organized in dfs.items():
        _df = pd.concat(organized)
        _df.columns = [col.translate(_trans_table) for col in _df.columns]
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

    dfs = csv_to_dfs(opts.input_dir)

    if opts.output:
        outfile = opts.output
    else:
        opts.type = "netcdf"
        outdir = opts.input_dir.rsplit("/", maxsplit=1)[0]
        outfile = f"{outdir}/data-Laurens-HyChain_WP1.nc"

    match opts.type:
        case "netcdf":
            print(f"Writing to NetCDF: {outfile}")
            ds = multi_indexed_dfs_to_xarray(dfs)
            ds.to_netcdf(outfile)
        case "sqlite":
            print(f"Writing to SQLite: {outfile}, table 'h2_scenarios'")
            con = sqlite3.connect(opts.output)
            for tbl, df in dfs.items():
                df.to_sql(tbl, con)
            con.close()
        case "duckdb":
            print(f"Writing to DuckDB: {outfile}, table 'h2_scenarios'")
            con = duckdb.connect(opts.output)
            for tbl, df in dfs.items():
                con.register(f"{tbl}_df", df.reset_index())
                con.sql(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM {tbl}_df")
            con.close()
