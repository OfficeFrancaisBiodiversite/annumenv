from scripts.annumenv import *
from scripts.psql_fun import *


with open('config/indicators.csv', newline='') as csvfile:
    treader = csv.DictReader(csvfile, delimiter=';', quotechar='"')
    try:
        for row in treader:
            try:
                print(row)
                if row["go"] == '0':
                    continue
                legend = Legend(row)
                legend.build_ref()
                indicator = Indicator(row,legend.getId())
                indicator.show()
                indicator.build_ref()
                ds = Result(row, indicator.getId())
                ds.build_ref()
                ds.vexploits_str()
                ds.vexploits_num()

            except() as error:
                print(error)
    finally:
        if conn is not None:
            conn.close()