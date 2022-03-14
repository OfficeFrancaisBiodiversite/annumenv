from scripts.annumenv import *



class CrossGrid():
    def __init__(self, trow):
        self.trow = trow
        self.id_grid = Grid(self.trow['work_grid']).findKey()

    def show(self):
        print("Preparing cross grid :"+self.trow['name'])

    def build_ref(self):
        #executeSql("REINDEX TABLE annumenv.cells", conn)
        executeSql("DROP TABLE IF EXISTS sas.\"_temp_{0}\"".format(self.id_grid), conn)
        executeSql("CREATE TABLE sas.\"_temp_{0}\" (cell_code text, geom geometry)".format(self.id_grid), conn)
        self.sqlInsertTable = "INSERT INTO sas.\"_temp_{6}\"" \
                              "WITH req as (SELECT cell_code, ST_INTERSECTION(a1.geom, a2.geom) as geom FROM annumenv.cells a1, \"{0}\".\"{1}\" a2 " \
                               "WHERE ST_INTERSECTS(a1.geom, a2.geom) AND id_cell BETWEEN {2} AND {3} {5} )" \
                               "SELECT cell_code, ST_UNION(ST_TRANSFORM(geom,{4})) as geom FROM req GROUP BY cell_code"
        print("Computing temp cross table with geometries from grid (safe mode)...")
        sqlGetMinMaxIdCells = "SELECT min(id_cell), max(id_cell) FROM annumenv.cells WHERE id_grid = {0} ".format(self.id_grid)
        id_cellExtr = getSql(sqlGetMinMaxIdCells, conn).fetchone()
        #id_cellMin = id_cellExtr[0]
        id_cellMin = 3794602
        id_cellMax = id_cellExtr[1]
        self.step = int(self.trow['limit_load_size'])
        condition = " AND "+self.trow["condition"] if self.trow["condition"] and len(self.trow["condition"])>0 else  ""

        for id_cell in range(id_cellMin, id_cellMax, self.step):
            t1 = datetime.now()
            id_cellMaxLoop = (id_cell + self.step - 1) if (id_cell + self.step - 1) < id_cellMax else id_cellMax
            print(str(round((id_cell - id_cellMin) / (id_cellMax - id_cellMin) * 100, 2))
                  + " - " + str(round((id_cellMaxLoop - id_cellMin) / (id_cellMax - id_cellMin) * 100, 2))
                  + "% (" + str(id_cell - id_cellMin) + ":" + str(id_cellMax - id_cellMin) + ")")
            executeSql(self.sqlInsertTable.format(
                self.trow["source_schema"],self.trow["source_table"],
                id_cell, id_cellMaxLoop,self.trow["srid"], condition, self.id_grid
            ),conn)
        #self.trow['update_mode'] = 'deleteall'
        self.trow['update_mode'] = 'adddiff'
        self.trow['source_schema'] = 'sas'
        self.trow['source_ucode'] = 'cell_code'
        self.trow['source_table'] = "_temp_{0}".format(self.id_grid)

        grid = Grid(self.trow)
        grid.show()
        grid.build_ref()
        print(datetime.now() - t1)
       # executeSql("DROP TABLE IF EXISTS sas.\"_temp_{0}\"".format(self.id_grid), conn)

with open('config/grids_cross.csv', newline='') as csvfile:
    treader = csv.DictReader(csvfile, delimiter=';', quotechar='"')

    try:
        for row in treader:
            try:
                if row["go"] == '0':
                    continue
                grid = CrossGrid(row)
                grid.show()
                grid.build_ref()
            except() as error:
                print(error)
    finally:
        if conn is not None:
            conn.close()