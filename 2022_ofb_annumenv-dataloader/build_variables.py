from scripts.annumenv import *

with open('config/variables.csv', newline='') as csvfile:
    treader = csv.DictReader(csvfile, delimiter=';', quotechar='"')
    try:
        for row in treader:
            try:
                if row["go"] == '0':
                    continue
                legend = Legend(row)
                legend.build_ref()
                variable = Variable(row,legend.getId())
                variable.show()
                variable.build_ref()
                ds = Dataset(row, variable.getId())
                ds.build_ref()
            except() as error:
                print(error)
    finally:
        if conn is not None:
            conn.close()