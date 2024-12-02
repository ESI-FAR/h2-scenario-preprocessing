from pathlib import Path

import pandas as pd
import parse


def csv_to_df(dirname: str):
    template = "{which_data}_{technology:l}_{distance:d}_{cost1:l}_{cost2:l}_{pipe_kind:l}_pipe.csv"
    dst = Path(dirname)
    csvs = list(dst.glob("**/*.csv"))
    dfs = [
        pd.read_csv(c, index_col=0).assign(**parse.parse(template, c.name).named)
        for c in csvs
    ]

    # debug
    for which_data in ["simp_output_results", "sistem costs", "time_dep_costs"]:
        num = set(len(d) for d in dfs if d["which_data"].unique()[0] == which_data)
        print(f"{which_data=}: {num=}")

    # FIXME: I'm not sure about the orientation of the DFs, can't concat
    # pd.concat(dfs, axis=1)
    return
