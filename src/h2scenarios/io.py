"""Data cleanup utilities."""

from collections import defaultdict
from pathlib import Path

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

        df = df.set_index(columns_to_index)
        dfs[which_data].append(df)

    # Merge them into one DataFrame per 'which_data'
    combined_dfs = {}
    for which_data, organized in dfs.items():
        combined_dfs[which_data] = pd.concat(organized)

    return combined_dfs


def multi_indexed_dfs_to_xarray(multi_indexed_dfs):
    """

    XArray merge each multi-indexed dataframe into a single Dataset:
    - the coordinates (multi-index) match between files
    - data columns become DataArrays

    :param multi_indexed_dfs:
    :return:
    """
    ds = xr.merge(xr.Dataset.from_dataframe(df) for df in multi_indexed_dfs.values())

    # '/' is not accepted as a variable name by NetCDF, rename to '_per_'
    renaming = {old: old.replace("/", "_per_") for old in ds.data_vars.keys()}
    ds = ds.rename_vars(renaming)
    return ds


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(__doc__)
    parser.add_argument("input_dir")
    opts = parser.parse_args()

    multi_indexed_dfs = csv_to_dfs(opts.input_dir)

    ds = multi_indexed_dfs_to_xarray(multi_indexed_dfs)

    # Export!
    outdir = opts.input_dir.rsplit("/", maxsplit=1)[0]
    print(f"Exporting to {outdir}")
    ds.to_netcdf(f"{outdir}/data-Laurens-HyChain_WP1.nc")
