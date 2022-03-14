from scripts.annumenv import *


with open('config/grids.csv', newline='') as csvfile:
    treader = csv.DictReader(csvfile, delimiter=';', quotechar='"')

    try:
        for row in treader:
            try:
                if row["go"] == '0':
                    continue
                grid = Grid(row)
                grid.show()
                grid.build_ref()
            except() as error:
                print(error)
    finally:
        if conn is not None:
            conn.close()