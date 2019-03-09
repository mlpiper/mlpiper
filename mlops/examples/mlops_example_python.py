
from parallelm.mlops import mlops
from parallelm.mlops.mlops_mode import MLOpsMode
from parallelm.mlops.stats.multi_line_graph import MultiLineGraph
from parallelm.mlops.stats.table import Table
from parallelm.mlops.stats.bar_graph import BarGraph


def main():
    print("Starting example")
    mlops.init(run_in_non_pm_mode=True, mlops_mode=MLOpsMode.PYTHON)

    # Line graphs
    mlops.set_stat("myCounterDouble", 5.5)
    mlops.set_stat("myCounterDouble2", 7.3)

    # Multi-line graphs
    mlt = MultiLineGraph().name("Multi Line").labels(["l1", "l2"]).data([5, 16])
    mlops.set_stat(mlt)

    tbl = Table().name("MyTable").cols(["Date", "Some number"])
    tbl.add_row(["2001Q1", "55"])
    tbl.add_row(["2001Q2", "66"])
    tbl.add_row(["2003Q3", "33"])
    tbl.add_row(["2003Q2", "22"])
    mlops.set_stat(tbl)

    bar = BarGraph().name("MyBar").cols(["aa", "bb", "cc", "dd", "ee"]).data([10, 15, 12, 9, 8])
    mlops.set_stat(bar)

    mlops.done()
    print("Example done")


if __name__ == "__main__":
    main()
