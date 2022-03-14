from scripts.psql_fun import *

with open('config/pg_config.csv', newline='') as pgfile:
    cpgfile = csv.reader(pgfile, delimiter=';', quotechar='"')
    pg_config = dict()
    for row in cpgfile:
        pg_config[row[0]] = row[1]
    conn = psycopg2.connect(
        host=pg_config["host"],
        port=pg_config["port"],
        database=pg_config["database"],
        user=pg_config["pg_user"],
        password=pg_config["pg_pwd"])
    user = pg_config["user_name"]

class Grid():

    def findKey(self):
        sqlFindKey = "SELECT id_grid FROM annumenv.grids WHERE \"name\" = '{0}' LIMIT 1".format(prepareStr_sql(self.trow['name']))
        res = getSql(sqlFindKey, conn).fetchone()
        if not res:
            return None
        else:
            return res[0]

    def __init__(self, trow):
        if type(trow) is str:
            self.name = trow
            self.trow = {'name': self.name}
        else:
            self.trow = trow

    def build_ref(self):
        id_ref = self.findKey()
        sqlDeleteGrid = "DELETE FROM annumenv.grids WHERE \"name\" = '{0}'"\
            .format(str(self.trow['name']).replace("'", "''"))
        sqlCreateGrid = "INSERT INTO annumenv.grids (\"name\",\"description\",\"grid_code\") VALUES ('{0}','{1}','{2}')" \
            .format(prepareStr_sql(self.trow['name']), prepareStr_sql(self.trow['description']), prepareStr_sql(self.trow['code']))
        if (not id_ref is None and 'deleteall' in self.trow['update_mode'].lower()) or (id_ref is None):
            executeSql(sqlDeleteGrid,conn)
            executeSql(sqlCreateGrid, conn)
            id_ref = self.findKey()
            sqlCreateCells = 'INSERT INTO annumenv.cells (cell_code,id_grid, geom) ' \
                             'SELECT "{0}" as  cell_code, {1} as id_grid, st_transform(geom,{4}) FROM "{2}"."{3}"' \
                .format(prepareStr_sql(self.trow['source_ucode']), id_ref, prepareStr_sql(self.trow['source_schema']), prepareStr_sql(self.trow['source_table']), self.trow['srid'])
            executeSql(sqlCreateCells,conn)
        elif 'adddiff' in self.trow['update_mode'].lower():
            sqlCreateCells = 'INSERT INTO annumenv.cells (cell_code,id_grid, geom)' \
                             'SELECT "{0}" as  cell_code, {1} as id_grid, st_transform(geom,{4}) FROM "{2}"."{3}" a1 ' \
                             'WHERE a1."{0}"NOT IN (SELECT DISTINCT cell_code FROM annumenv.cells WHERE id_grid = {1})' \
                .format(prepareStr_sql(self.trow['source_ucode']), id_ref, prepareStr_sql(self.trow['source_schema']),
                        prepareStr_sql(self.trow['source_table']), self.trow['srid'])
            executeSql(sqlCreateCells, conn)
        elif 'updategeom' in self.trow['update_mode'].lower():
            sqlCreateCells = 'UPDATE annumenv.cells SET geom = st_transform(a1.geom,{4}) FROM "{2}"."{3}" a1 WHERE cell_code = a1."{0}" AND "id_grid" = {1}'\
                .format(prepareStr_sql(self.trow['source_ucode']), id_ref, prepareStr_sql(self.trow['source_schema']),
                        prepareStr_sql(self.trow['source_table']), self.trow['srid'])
            executeSql(sqlCreateCells, conn)

    def show(self):
        print("Preparing grid :"+self.trow['name'])



class Legend():
    def __init__(self, trow):
        self.trow = trow
    def findKey(self):
        sqlFindKey = "SELECT id_legend FROM annumenv.legends WHERE \"content\" = '{0}' LIMIT 1".format(
            prepareStr_sql(self.trow['legend']))
        res = getSql(sqlFindKey, conn).fetchone()
        if not res:
            return None
        else:
            return res[0]
    def show(self):
        print("Preparing legend :"+self.trow['name'])
    def build_ref(self):
        if self.findKey() is None:
            sqlCreateLegend = "INSERT INTO annumenv.legends (\"content\",\"id_legend_type\") VALUES ('{0}',1)" \
                .format(prepareStr_sql(self.trow['legend']))
            executeSql(sqlCreateLegend, conn)
    def getId(self):
        return self.findKey()


class Variable():
    def __init__(self, trow, id_legend):
        if type(trow) is str:
            self.name = trow
            self.trow = {'name':self.name}
        else :
            self.trow = trow
        self.id_legend = id_legend
    def findKey(self):
        sqlFindKey = "SELECT id_variable FROM annumenv.variables WHERE \"name\" = '{0}' LIMIT 1".format(
            prepareStr_sql(self.trow['name']))
        res = getSql(sqlFindKey, conn).fetchone()
        if not res:
            return None
        else:
            return res[0]
    def show(self):
        print("Preparing variable : "+self.trow['name'])
    def getId(self):
        return self.findKey()
    def build_ref(self):
        if self.findKey() is None:
            sqlCreateVariable = "INSERT INTO annumenv.variables (\"name\",\"id_legend\",\"info\") VALUES ('{0}',{1},'{1}'::jsonb)" \
                .format(prepareStr_sql(self.trow['name']),self.id_legend,prepareStr_sql(self.trow['json_info']))
            executeSql(sqlCreateVariable, conn)


class Dataset():
    def __init__(self, trow, id_variable):
        self.trow = trow
        self.id_variable = id_variable
    def show(self):
        print("Preparing dataset :"+self.trow['name'])

    def build_ref_date(self, date_ref, partition_sqldate, seekDate = None):
        if self.trow['source_condition'] and len(str(self.trow['source_condition'])) != 0:
            self.condition = " WHERE " + self.trow['source_condition']
        if seekDate and self.condition == '' :
            self.condition = ' WHERE "{0}" = \'{1}\''.format(seekDate, date_ref)
        elif seekDate :
            self.condition += ' AND "{0}" = \'{1}\''.format(seekDate, date_ref)

        value_num = "NULL"
        value_str = '"' + prepareStr_sql(self.trow['source_column']) + '"::text'

        if str(self.trow['is_numeric']).lower() == "true":
            value_num = '"' + self.trow['source_column'] + '"::numeric'
            value_str = "NULL"

        if self.trow['source_column'] == "" or len(self.trow['source_column']) == 0:
            value_num = "NULL"
            value_str = "NULL"

        if self.trow['use_field_str'] and len(str(self.trow['use_field_str']).lower()) > 0:
            value_str = '"' + prepareStr_sql(self.trow['use_field_str']) + '"::text'

        json4sql_source_expression = "NULL"
        if self.trow['json4sql_source_expression'] and len(str(self.trow['json4sql_source_expression'])) != 0:
            json4sql_source_expression = "(" + self.trow['json4sql_source_expression'] + ")::jsonb"
        if not check_table("annumenv_part_datasets", "datasets_" + str(self.id_variable), conn):
            sqlCreatePartition = "CREATE TABLE annumenv_part_datasets.datasets_{0} " \
                                 "PARTITION OF annumenv.datasets FOR VALUES IN ({0}) " \
                                 "PARTITION BY LIST(date_result)".format(
                self.id_variable
            )
            executeSql(sqlCreatePartition, conn)
        if check_table("annumenv_part_datasets", "datasets_" + str(self.id_variable) + "_" + partition_sqldate, conn):
            #executeSql("'DELETE FROM annumenv.datasets WHERE id_variable = {0} AND date_result = '{1}'".format(self.id_variable), conn)
            sqlDropPartition = "DROP TABLE annumenv_part_datasets.datasets_{0}_{1}".format(
                self.id_variable, partition_sqldate)
            executeSql(sqlDropPartition, conn)
        sqlCreatePartition = "CREATE TABLE annumenv_part_datasets.datasets_{0}_{1} " \
                             "PARTITION OF annumenv_part_datasets.datasets_{0} FOR VALUES IN ('{2}')".format(
            self.id_variable,
            partition_sqldate,
            date_ref
        )
        executeSql(sqlCreatePartition, conn)

        sqlCreateDataset = 'INSERT INTO annumenv.datasets(date_result, id_variable, geom, value_str, value_num, "info") ' \
                           'SELECT \'{0}\', {1}, source.geom, {2}, {3}, {4} ' \
                           'FROM "{5}"."{6}" source {7}'.format(
            date_ref,
            self.id_variable,
            value_str,
            value_num,
            json4sql_source_expression,
            self.trow['source_schema'],
            self.trow['source_table'],
            self.condition
        )
        executeSql(sqlCreateDataset, conn)
        sqlUpdate = "INSERT INTO annumenv.updates(date_result,datasets_update, date_real, feeduser) VALUES(\'{0}\',{1}, now(), '{2}')".format(
            date_ref, '\'{' + str(self.id_variable) + '}\'::bigint[]', prepareStr_sql(user))
        executeSql(sqlUpdate, conn)

    def build_ref(self):
        self.condition = ""
        if testDateFu(self.trow['reference_date'],'%d/%m/%Y'):
            self.build_ref_date(self.trow['reference_date'], datetime.strptime(self.trow['reference_date'],'%d/%m/%Y').strftime('%Y%m%d'))
        else :
            for cursor in getSql("SELECT DISTINCT \"{0}\" FROM \"{1}\".\"{2}\" ORDER BY \"{0}\"".format(
                self.trow['reference_date'], self.trow['source_schema'],self.trow['source_table']), conn):
                print(cursor[0])
                self.condition = ""
                self.build_ref_date(cursor[0], cursor[0].strftime('%Y%m%d'),self.trow['reference_date'])


class Indicator():
    def __init__(self, trow, id_legend):
        if type(trow) is str:
            self.name = trow
            self.trow = {'name': self.name}
        else:
            self.trow = trow
            self.id_legend = id_legend
    def findKey(self):
        sqlFindKey = "SELECT id_indicator FROM annumenv.indicators WHERE \"name\" = '{0}' LIMIT 1".format(
            prepareStr_sql(self.trow['name']))
        res = getSql(sqlFindKey, conn).fetchone()
        if not res:
            return None
        else:
            return getSql(sqlFindKey, conn).fetchone()[0]
    def show(self):
        print("Preparing indicator : "+self.trow['name'])
    def getId(self):
        return self.findKey()
    def build_ref(self):
        if self.findKey() is None:
            sqlCreateVariable = "INSERT INTO annumenv.indicators (\"name\",\"id_legend\",\"info\") VALUES ('{0}',{1},'{1}'::jsonb)" \
                .format(prepareStr_sql(self.trow['name']),self.id_legend,prepareStr_sql(self.trow['json_info']))
            executeSql(sqlCreateVariable, conn)


class Result():

    def __init__(self, trow, id_indicator):
        if not trow or type(trow) is str:
            self.id_indicator = id_indicator
        else:
            self.trow = trow
            self.id_indicator = id_indicator
            self.id_variable = Variable(self.trow['work_variable'],0).findKey()
            self.id_grid = Grid(self.trow['work_grid']).findKey()
        self.sqlModeOrders = {
            "intersect" : "sum(intersector)",
            "intersect/1000": "sum(intersector)/1000",
            "prop_surf":"sum(cell_prop)",
            "prop_len": "sum(cell_prop)",
            "value_str_mode_ct_pt": "NULL",
            "value_invdist_pt": "NULL",
            "md_prop": "sum(value_num * cell_prop)",
            "md_prop_norm": "sum(value_num * cell_prop) / sum(cell_prop)",
            "tot_dens": "sum(cellvalue_density)",
            "val_sum": "sum(value_num)",
            "val_min": "min(value_num)",
            "val_max": "max(value_num)",
            "val_avg": "avg(value_num)",
            "val_stddev": "stddev(value_num)",
            "val_median": "median(value_num)",
            "plus_stats": "'{\"avg_value\":\"' ||COALESCE(round(avg(value_num),4)::text,'') ||'\",\"min_value\":\"' ||COALESCE(round(min(value_num),4)::text,'') ||'\",\"max_value\":\"' ||COALESCE(round(max(value_num),4)::text,'') ||'\",\"stddev_value\":\"' ||COALESCE(round(stddev(value_num),4)::text,'') ||'\",\"median_value\":\"' ||COALESCE(round(median(value_num),4)::text,'') ||'\",\"count\":\"' ||COALESCE(count(value_num)::text,'') ||'\"}'::text",
            "value_str_mode_inter": "(SELECT DISTINCT value_str::text FROM stats st2 WHERE stats.id_cell=st2.id_cell AND cell_prop IN(SELECT max(st3.cell_prop) FROM stats st3 WHERE stats.id_cell = st3.id_cell) LIMIT 1)",
            "ct_elt": "count(*)",
            "ct_elt_num": "count(distinct value_num)",
            "ct_elt_str": "count(distinct value_str)",
            "value_full": "array_agg(DISTINCT '{\"value_num\":\"' || COALESCE(value_num::text,'') || '\",\"value_str\":\"' || COALESCE(value_str,'') || '\",\"cell_prop\":\"' || COALESCE(cell_prop::text,'') || '\",\"cellvalue_density\":\"' || COALESCE(cellvalue_density::text,'') || '\"}')::text",
            "value_str_full": "array_agg(DISTINCT '{\"value_str\":\"' || COALESCE(value_str,'') || '\",\"cell_prop\":\"' || COALESCE(cell_prop::text,'') || '\"}')::text",
            "value_num_agg": "array_agg(DISTINCT value_num ORDER BY value_num)::text",
            "value_str_agg": "array_agg(DISTINCT value_str ORDER BY value_str)::text",
            "": "NULL",
            None: "NULL"
        }

    def show(self):
        print("Preparing results of indicators :"+self.trow['name'])

    def vexploits_num(self, exploits_grid = None, init_grid = None):

        print("Generate exploits NUM...")
        nbNn = getSql("SELECT count(*) FROM annumenv.indicator_results  WHERE id_indicator = {0} AND value_num IS NOT NULL".format(self.id_indicator),conn).fetchone()[0]
        if nbNn == 0:
            return

        try:
            self.id_grid = getSql(
            "SELECT id_grid FROM annumenv.indicator_results JOIN annumenv.cells USING(id_cell) WHERE id_indicator = {0} LIMIT 1".format(
                self.id_indicator), conn).fetchone()[0]
        except:
            return

        sqlDropPartition = "DROP VIEW IF EXISTS annumenv_exploits.vindicator_num_{0}".format(self.id_indicator)
        executeSql(sqlDropPartition, conn)

        self.sql_1a = "CREATE VIEW annumenv_exploits.vindicator_num_{0} AS SELECT id_cell, cell_code, geom ".format(
            self.id_indicator)
        self.sql_1b = ", (ARRAY_AGG(res.value_num) FILTER (WHERE res.date_result = '{1}'))[1] AS value_{0}"
        self.sql_2a = " FROM annumenv.cells JOIN annumenv.indicator_results res USING(id_cell)" \
                      " WHERE id_grid = {0}  AND id_indicator = {1} AND value_num IS NOT NULL " \
                      " GROUP BY  id_cell, cell_code, geom".format(self.id_grid, self.id_indicator)



        for cursor in getSql(
                "SELECT DISTINCT date_result FROM annumenv.indicator_results WHERE id_indicator = {0} ORDER BY date_result".format(
                        self.id_indicator), conn):
            partition_sqldate = cursor[0].strftime('%Y%m%d')
            self.sql_1a += self.sql_1b.format(partition_sqldate, cursor[0])
        #print(self.sql_1a + self.sql_2a)
        executeSql(self.sql_1a + self.sql_2a, conn)

#        for sql_e in self.sql_4a:
#           executeSql(sql_e.format(self.id_indicator), conn)

        exploits_grid = self.trow["exploits_grid"] if hasattr(self, 'trow') else None
        if exploits_grid and len(exploits_grid) > 0:
            id_gridexpl = Grid(exploits_grid).findKey()
            executeSql("DROP VIEW IF EXISTS annumenv_exploits.vindicator_num_{0}_{1}".format(
                self.id_indicator, id_gridexpl
            ), conn)
            executeSql("CREATE VIEW annumenv_exploits.vindicator_num_{0}_{1} AS SELECT {2}, a2.geom "
                       "FROM annumenv_exploits.indicator_num_{0} a1 "
                       "JOIN (SELECT * FROM annumenv.cells WHERE id_grid = {1}) a2 USING(cell_code)".format(
                self.id_indicator, id_gridexpl, self.sql_5
            ), conn)


    def exploits_num(self, exploits_grid = None, init_grid = None):

        print("Generate exploits NUM...")
        nbNn = getSql("SELECT count(*) FROM annumenv.indicator_results  WHERE id_indicator = {0} AND value_num IS NOT NULL".format(self.id_indicator),conn).fetchone()[0]
        if nbNn == 0:
            return

        try:
            self.id_grid = getSql(
            "SELECT id_grid FROM annumenv.indicator_results JOIN annumenv.cells USING(id_cell) WHERE id_indicator = {0} LIMIT 1".format(
                self.id_indicator), conn).fetchone()[0]
        except:
            return

        sqlDropPartition = "DROP TABLE IF EXISTS annumenv_exploits.indicator_num_{0}".format(self.id_indicator)
        executeSql(sqlDropPartition, conn)

        self.sql_1a = "CREATE TABLE annumenv_exploits.indicator_num_{0} AS SELECT id_cell, cell_code, geom ".format(
            self.id_indicator)
        self.sql_1b = ", (ARRAY_AGG(res.value_num) FILTER (WHERE res.date_result = '{1}'))[1] AS value_{0}"
        self.sql_2a = " FROM annumenv.cells JOIN annumenv.indicator_results res USING(id_cell)" \
                      " WHERE id_grid = {0}  AND id_indicator = {1} AND value_num IS NOT NULL " \
                      " GROUP BY  id_cell, cell_code, geom".format(self.id_grid, self.id_indicator)

        self.sql_4a = ["CREATE INDEX ON annumenv_exploits.indicator_num_{0}(id_cell)",
                       "CREATE INDEX ON annumenv_exploits.indicator_num_{0}(cell_code)",
                       "CREATE INDEX ON annumenv_exploits.indicator_num_{0} USING GIST(geom)"]
        self.sql_4b = ["CREATE INDEX ON annumenv_exploits.indicator_num_{0}_{1}(id_cell)",
                       "CREATE INDEX ON annumenv_exploits.indicator_num_{0}_{1}(cell_code)",
                       "CREATE INDEX ON annumenv_exploits.indicator_num_{0}_{1} USING GIST(geom)"]

        for cursor in getSql(
                "SELECT DISTINCT date_result FROM annumenv.indicator_results WHERE id_indicator = {0} ORDER BY date_result".format(
                        self.id_indicator), conn):
            partition_sqldate = cursor[0].strftime('%Y%m%d')
            self.sql_1a += self.sql_1b.format(partition_sqldate, cursor[0])
        #print(self.sql_1a + self.sql_2a)
        executeSql(self.sql_1a + self.sql_2a, conn)

        for sql_e in self.sql_4a:
            executeSql(sql_e.format(self.id_indicator), conn)

        exploits_grid = self.trow["exploits_grid"] if hasattr(self, 'trow') else None
        if exploits_grid and len(exploits_grid) > 0:
            id_gridexpl = Grid(exploits_grid).findKey()
            executeSql("DROP TABLE IF EXISTS annumenv_exploits.indicator_num_{0}_{1}".format(
                self.id_indicator, id_gridexpl
            ), conn)
            executeSql("CREATE TABLE annumenv_exploits.indicator_num_{0}_{1} AS SELECT {2}, a2.geom "
                       "FROM annumenv_exploits.indicator_num_{0} a1 "
                       "JOIN (SELECT * FROM annumenv.cells WHERE id_grid = {1}) a2 USING(cell_code)".format(
                self.id_indicator, id_gridexpl, self.sql_5
            ), conn)
            for sql_e in self.sql_4b:
                executeSql(sql_e.format(self.id_indicator, id_gridexpl), conn)
    def vexploits_str(self, exploits_grid = None):
        print("Generate exploits STR...")
        nbNn = getSql("SELECT count(*) FROM annumenv.indicator_results  WHERE id_indicator = {0} AND value_str IS NOT NULL".format(self.id_indicator), conn).fetchone()[0]
        if nbNn == 0:
            return

        try:
            self.id_grid = getSql("SELECT id_grid FROM annumenv.indicator_results JOIN annumenv.cells USING(id_cell) WHERE id_indicator = {0} LIMIT 1".format(self.id_indicator), conn).fetchone()[0]
        except:
            return

        sqlDropPartition = "DROP VIEW IF EXISTS annumenv_exploits.vindicator_str_{0}".format(self.id_indicator)
        executeSql(sqlDropPartition, conn)
        self.sql_1a = "CREATE VIEW annumenv_exploits.vindicator_str_{0} AS SELECT id_cell, cell_code, geom ".format(self.id_indicator)
        self.sql_1b = ", (ARRAY_AGG(res.value_str) FILTER (WHERE res.date_result = '{1}'))[1] AS value_{0}"
        self. sql_2a = " FROM annumenv.cells JOIN annumenv.indicator_results res USING(id_cell)" \
                       " WHERE id_grid = {0}  AND id_indicator = {1} AND value_str IS NOT NULL " \
                       " GROUP BY  id_cell, cell_code, geom".format(self.id_grid,self.id_indicator)


        for cursor in getSql("SELECT DISTINCT date_result FROM annumenv.indicator_results WHERE id_indicator = {0} ORDER BY date_result".format(self.id_indicator), conn):
            partition_sqldate = cursor[0].strftime('%Y%m%d')
            self.sql_1a += self.sql_1b.format(partition_sqldate, cursor[0])
        #print(self.sql_1a + self.sql_2a)
        executeSql(self.sql_1a + self.sql_2a, conn)
#        for sql_e in  self.sql_4a:
#           executeSql(sql_e.format(self.id_indicator), conn)

        exploits_grid = self.trow["exploits_grid"] if hasattr(self, 'trow') else None
        if exploits_grid and len(exploits_grid)>0:
            id_gridexpl = Grid(exploits_grid).findKey()
            executeSql("DROP VIEW IF EXISTS annumenv_exploits.vindicator_str_{0}_{1}".format(
                self.id_indicator, id_gridexpl
            ), conn)
            executeSql("CREATE VIEW annumenv_exploits.vindicator_str_{0}_{1} AS SELECT {2}, a2.geom "
                       "FROM annumenv_exploits.indicator_str_{0} a1 "
                       "JOIN (SELECT * FROM annumenv.cells WHERE id_grid = {1}) a2 USING(cell_code)".format(
                self.id_indicator, id_gridexpl, self.sql_5
            ), conn)

    def exploits_str(self, exploits_grid = None):
        print("Generate exploits STR...")
        nbNn = getSql("SELECT count(*) FROM annumenv.indicator_results  WHERE id_indicator = {0} AND value_str IS NOT NULL".format(self.id_indicator), conn).fetchone()[0]
        if nbNn == 0:
            return

        try:
            self.id_grid = getSql("SELECT id_grid FROM annumenv.indicator_results JOIN annumenv.cells USING(id_cell) WHERE id_indicator = {0} LIMIT 1".format(self.id_indicator), conn).fetchone()[0]
        except:
            return

        sqlDropPartition = "DROP TABLE IF EXISTS annumenv_exploits.indicator_str_{0}".format(self.id_indicator)
        executeSql(sqlDropPartition, conn)
        self.sql_1a = "CREATE TABLE annumenv_exploits.indicator_str_{0} AS SELECT id_cell, cell_code, geom ".format(self.id_indicator)
        self.sql_1b = ", (ARRAY_AGG(res.value_str) FILTER (WHERE res.date_result = '{1}'))[1] AS value_{0}"
        self. sql_2a = " FROM annumenv.cells JOIN annumenv.indicator_results res USING(id_cell)" \
                       " WHERE id_grid = {0}  AND id_indicator = {1} AND value_str IS NOT NULL " \
                       " GROUP BY  id_cell, cell_code, geom".format(self.id_grid,self.id_indicator)

        self.sql_4a = [  "CREATE INDEX ON annumenv_exploits.indicator_str_{0}(id_cell)",
                        "CREATE INDEX ON annumenv_exploits.indicator_str_{0}(cell_code)",
                        "CREATE INDEX ON annumenv_exploits.indicator_str_{0} USING GIST(geom)"]
        self.sql_4b = [  "CREATE INDEX ON annumenv_exploits.indicator_str_{0}_{1}(id_cell)",
                        "CREATE INDEX ON annumenv_exploits.indicator_str_{0}_{1}(cell_code)",
                        "CREATE INDEX ON annumenv_exploits.indicator_str_{0}_{1} USING GIST(geom)"]

        for cursor in getSql("SELECT DISTINCT date_result FROM annumenv.indicator_results WHERE id_indicator = {0} ORDER BY date_result".format(self.id_indicator), conn):
            partition_sqldate = cursor[0].strftime('%Y%m%d')
            self.sql_1a += self.sql_1b.format(partition_sqldate, cursor[0])
        #print(self.sql_1a + self.sql_2a)
        executeSql(self.sql_1a + self.sql_2a, conn)
        for sql_e in  self.sql_4a:
            executeSql(sql_e.format(self.id_indicator), conn)

        exploits_grid = self.trow["exploits_grid"] if hasattr(self, 'trow') else None
        if exploits_grid and len(exploits_grid)>0:
            id_gridexpl = Grid(exploits_grid).findKey()
            executeSql("DROP TABLE IF EXISTS annumenv_exploits.indicator_str_{0}_{1}".format(
                self.id_indicator, id_gridexpl
            ), conn)
            executeSql("CREATE TABLE annumenv_exploits.indicator_str_{0}_{1} AS SELECT {2}, a2.geom "
                       "FROM annumenv_exploits.indicator_str_{0} a1 "
                       "JOIN (SELECT * FROM annumenv.cells WHERE id_grid = {1}) a2 USING(cell_code)".format(
                self.id_indicator, id_gridexpl, self.sql_5
            ), conn)
            for sql_e in  self.sql_4b:
                executeSql(sql_e.format(self.id_indicator, id_gridexpl), conn)

    def getsql_ref_lines(self):
        self.step = 1000
        if self.trow['turbo_compute'].lower() == "true":
            print("Computing indicator with geometry cross (turbo mode, lines)...")
            return "INSERT INTO annumenv.indicator_results(date_result, id_indicator,id_cell,value_str,value_num)" \
                                      "WITH inter_lengths AS (" \
                                      "SELECT ST_LENGTH(ST_UNION(ST_INTERSECTION(cells.geom, datasets.geom))) as inter_area, " \
                                      "value_num/ST_LENGTH(datasets.geom) AS var_density, ST_LENGTH(datasets.geom) AS var_area,  id_cell, value_num, value_str FROM annumenv.datasets,annumenv.cells " \
                                      "WHERE  id_grid = {0} AND id_variable = {1} AND date_result = '{2}' {6} AND ST_INTERSECTS(cells.geom, datasets.geom)" \
                                      "GROUP BY id_cell, value_num, value_str, datasets.geom" \
                                      "), stats AS (" \
                                      "SELECT inter_area/sum(inter_area) as cell_prop, inter_area/var_area as var_prop, " \
                                      "id_cell, value_num, value_str, var_density as density, var_density*inter_area as cellvalue_density FROM inter_areas)" \
                                      "SELECT '{2}', {3}, id_cell, {4} as value_str, {5} as value_num FROM stats GROUP BY id_cell";
        else:
            print("Computing indicator with geometry cross (safe mode, lines)...")
            return "INSERT INTO annumenv.indicator_results(date_result, id_indicator,id_cell,value_str,value_num)" \
                                "WITH inter_areas_b AS (" \
                                "SELECT ST_INTERSECTION(cells.geom, datasets.geom) as inter_area, " \
                                "value_num/ST_LENGTH(datasets.geom) AS var_density, ST_LENGTH(datasets.geom) AS var_area, id_cell, value_num, value_str FROM annumenv.datasets,annumenv.cells " \
                                "WHERE  id_grid = {0} AND id_variable = {1} AND date_result = '{2}' {8} " \
                                "AND id_cell BETWEEN {6} AND {7} " \
                                "AND ST_INTERSECTS(cells.geom, datasets.geom)" \
                                "), inter_areas AS (" \
                                "SELECT ST_LENGTH(ST_UNION(inter_area)) as inter_area, var_area, var_density,  id_cell, value_num, value_str FROM inter_areas_b " \
                                "GROUP BY var_area, id_cell, value_num, value_str, var_density" \
                                "), stats2 AS (" \
                                "SELECT SUM(inter_area) as cell_area, id_cell FROM inter_areas GROUP BY id_cell" \
                                "), stats AS (" \
                                "SELECT inter_area/cell_area " \
                   "as cell_prop, inter_area/var_area as var_prop," \
                                "inter_area as intersector, id_cell, value_num, value_str, var_density as density, var_density*inter_area as cellvalue_density FROM inter_areas JOIN stats2 USING(id_cell) WHERE cell_area != 0) " \
                                "SELECT '{2}', {3}, id_cell, {4} as value_str, {5} as value_num FROM stats GROUP BY id_cell";

    def getsql_ref_polygs(self):
        self.step = 1000
        if self.trow['turbo_compute'].lower() == "true":
            print("Computing indicator with geometry cross (turbo mode, polygones)...")
            return "INSERT INTO annumenv.indicator_results(date_result, id_indicator,id_cell,value_str,value_num)" \
                                          "WITH inter_areas AS (" \
                                          "SELECT ST_AREA(ST_UNION(ST_INTERSECTION(cells.geom, datasets.geom))) as inter_area, " \
                                          "value_num/ST_AREA(datasets.geom) AS var_density, ST_AREA(datasets.geom) AS var_area, ST_AREA(cells.geom) AS cell_area,  id_cell, value_num, value_str FROM annumenv.datasets,annumenv.cells " \
                                          "WHERE  id_grid = {0} AND id_variable = {1} AND date_result = '{2}' {6} AND ST_INTERSECTS(cells.geom, datasets.geom)" \
                                          "GROUP BY id_cell, value_num, value_str, datasets.geom" \
                                          "), stats AS (" \
                                          "SELECT inter_area/cell_area as cell_prop, inter_area/var_area as var_prop, " \
                                          "inter_area as intersect, id_cell, value_num, value_str, var_density as density, var_density*inter_area as cellvalue_density FROM inter_areas)" \
                                          "SELECT '{2}', {3}, id_cell, {4} as value_str, {5} as value_num FROM stats GROUP BY id_cell";
        else:
            print("Computing indicator with geometry cross (safe mode, polygones)...")
            return "INSERT INTO annumenv.indicator_results(date_result, id_indicator,id_cell,value_str,value_num)" \
                                    "WITH inter_areas_b AS (" \
                                    "SELECT ST_INTERSECTION(cells.geom, datasets.geom) as inter_area, " \
                                    "value_num/ST_AREA(datasets.geom) AS var_density, ST_AREA(datasets.geom) AS var_area, ST_AREA(cells.geom) AS cell_area,  id_cell, value_num, value_str FROM annumenv.datasets,annumenv.cells " \
                                    "WHERE  id_grid = {0} AND id_variable = {1} AND date_result = '{2}' {8} " \
                                    "AND id_cell BETWEEN {6} AND {7} " \
                                    "AND ST_INTERSECTS(cells.geom, datasets.geom)" \
                                    "), inter_areas AS (" \
                                    "SELECT ST_AREA(ST_UNION(inter_area)) as inter_area, var_area, var_density, cell_area, id_cell, value_num, value_str FROM inter_areas_b " \
                                    "GROUP BY var_area, cell_area, id_cell, value_num, value_str, var_density" \
                                    "), stats AS (" \
                                    "SELECT inter_area/cell_area as cell_prop, inter_area/var_area as var_prop," \
                                    "inter_area as intersector, id_cell, value_num, value_str, var_density as density, var_density*inter_area as cellvalue_density FROM inter_areas)" \
                                    "SELECT '{2}', {3}, id_cell, {4} as value_str, {5} as value_num FROM stats GROUP BY id_cell";

    def getsql_ref_points(self):
        self.step = 10000
        sql_num = ""
        sql_str = ""
        sql_com = "SELECT date_result, {3} as id_indicator, id_cell, {4} as value_str, {5} as value_num " \
                 "FROM annumenv.cells, annumenv.datasets " \
                 "WHERE date_result = '{2}' {8} AND id_variable = {1} AND id_cell BETWEEN {6} AND {7} " \
                 "AND ST_INTERSECTS(cells.geom, datasets.geom)" \
                 "GROUP BY date_result, id_cell"

        if self.trow['num_mode'].lower().startswith('value_invdist_pt'):
            nb_sk = self.trow['num_mode'].split('#') if '#' in self.trow['num_mode'] else 8
            sql_num =  " SELECT date_result, {3} as id_indicator,id_cell, sum(var/dist)/sum(1/dist) as value_num" \
                       " FROM (SELECT date_result,id_cell, cell_code, var, dist, cells2.geom FROM annumenv.cells cells2" \
                       " CROSS JOIN LATERAL (" \
                       " SELECT date_result,value_num as var, st_centroid(cells2.geom) <-> subu.geom AS dist FROM annumenv.datasets AS subu " \
                       " WHERE date_result = '{2}' {8} AND id_variable = {1} ORDER BY dist LIMIT "+str(nb_sk)+") subu " \
                        "WHERE id_grid = {0} AND id_cell BETWEEN {6} AND {7}) " \
                       "rgeom GROUP BY id_cell, cell_code, date_result "

        if self.trow['str_mode'].lower() == 'value_str_mode_ct_pt':
            sql_str =   "SELECT date_result, {3} as id_indicator,id_cell, valmode as value_str FROM annumenv.cells cells2 " \
                        "CROSS JOIN LATERAL (SELECT date_result,value_str as valmode, count(*) as var " \
                        "FROM annumenv.datasets AS subu " \
                        "WHERE date_result = '{2}' {8} AND id_variable = {1} AND ST_Intersects(cells2.geom,subu.geom)  " \
                        "GROUP BY date_result,id_cell, cell_code, valmode ORDER BY var DESC LIMIT 1 ) subu " \
                        "WHERE id_grid = {0}  AND id_cell BETWEEN {6} AND {7}"

        if self.trow['str_mode'].lower() == 'prop_elt ':
            sql_str =   "SELECT date_result, {3} as id_indicator,id_cell, valmode as value_str FROM annumenv.cells cells2 " \
                        "CROSS JOIN LATERAL (SELECT date_result,value_str as valmode, count(*) as var " \
                        "FROM annumenv.datasets AS subu " \
                        "WHERE date_result = '{2}' {8} AND id_variable = {1} AND ST_Intersects(cells2.geom,subu.geom)  " \
                        "GROUP BY date_result,id_cell, cell_code, valmode ORDER BY var DESC LIMIT 1 ) subu " \
                        "WHERE id_grid = {0}  AND id_cell BETWEEN {6} AND {7}"

        if sql_num == '' and sql_str == '':
            return "INSERT INTO annumenv.indicator_results(date_result, id_indicator,id_cell,value_str, value_num)" \
                   " {0}".format(sql_com)
        if self.trow['str_mode'] == "" :
            return "INSERT INTO annumenv.indicator_results(date_result, id_indicator,id_cell,value_str)" \
                   " {0}".format(sql_str)
        if self.trow['num_mode'] == "" :
            return "INSERT INTO annumenv.indicator_results(date_result, id_indicator,id_cell,value_num)" \
                   " {0}".format(sql_num)
        if sql_num != "" and sql_str != "":
            return "INSERT INTO annumenv.indicator_results(date_result, id_indicator,id_cell,value_str,value_num)" \
                   " WITH a1 as ({0}), a2 as ({1})" \
                   "SELECT date_result, id_indicator,id_cell,value_str,value_num " \
                   "FROM a1 FULL JOIN a2 USING (date_result, id_indicator,id_cell)".format(sql_num, sql_str)
        if sql_num != "" and self.trow['str_mode'] != "":
            return "INSERT INTO annumenv.indicator_results(date_result, id_indicator,id_cell,value_str,value_num)" \
                   " WITH a1 as ({0}), a2 as ({1})" \
                   "SELECT date_result, id_indicator,id_cell,a2.value_str,a1.value_num " \
                   "FROM a1 FULL JOIN a2 USING (date_result, id_indicator,id_cell)".format(sql_num, sql_com)
        if sql_str != "" and self.trow['num_mode'] != "":
            return "INSERT INTO annumenv.indicator_results(date_result, id_indicator,id_cell,value_str,value_num)" \
                   " WITH a1 as ({0}), a2 as ({1})" \
                   "SELECT date_result, id_indicator,id_cell,a2.value_str,a1.value_num " \
                   "FROM a1 FULL JOIN a2 USING (date_result, id_indicator,id_cell)".format(sql_com, sql_str)


    def getsql_ref(self, typeGeom):
        typeGeom = typeGeom.lower()
        if "polygon" in typeGeom.lower():
            return self.getsql_ref_polygs()
        if "line" in typeGeom.lower():
            return self.getsql_ref_lines()
        if "point" in typeGeom.lower():
            return self.getsql_ref_points()

    def build_ref(self):
        #executeSql("REINDEX TABLE annumenv.cells", conn)
        executeSql("DELETE FROM annumenv.indicator_results WHERE id_indicator = {0}".format(self.id_indicator), conn)
        if self.trow['go'] == '1':
            sqlDropPartition = "DROP TABLE IF EXISTS annumenv_part_indicators.indicators_{0}".format(self.id_indicator)
            executeSql(sqlDropPartition, conn)
            sqlCreatePartition = "CREATE TABLE annumenv_part_indicators.indicators_{0} " \
                                 "PARTITION OF annumenv.indicator_results FOR VALUES IN ({0}) " \
                                 "PARTITION BY LIST(date_result)".format(self.id_indicator)
            executeSql(sqlCreatePartition, conn)
        sqlInsertResults = self.getsql_ref(getSql("SELECT st_GeometryType(geom) FROM annumenv.datasets WHERE id_variable = {0} AND geom IS NOT NULL LIMIT 1".format(self.id_variable), conn).fetchone()[0])

        conditionVariable = ""
        if self.trow['condition_variable'] and len(str(self.trow['condition_variable']))>0:
            conditionVariable = " AND "+self.trow['condition_variable']

        for cursor in getSql("SELECT DISTINCT date_result FROM annumenv.datasets WHERE id_variable = {0}".format(self.id_variable),conn):
            date_ref = cursor[0].strftime('%Y-%m-%d %H:%M:%S')
            print("Indicator for date : "+date_ref)
            partition_sqldate = cursor[0].strftime('%Y%m%d')
            if check_table("annumenv_part_indicators", "indicators_" + str(self.id_indicator)+"_"+partition_sqldate, conn):
                if self.trow['go'] == '2':
                    sql_ch_date = "SELECT id_cell FROM annumenv.cells a1 JOIN annumenv_part_datasets.datasets_{2} a2 " \
                                  "ON ST_INTERSECTS(a1.geom, a2.geom) AND a2.date_result = '{3}' AND id_grid = {4} " \
                                  "WHERE id_cell NOT IN (SELECT id_cell FROM annumenv_part_indicators.indicators_{0}_{1}) " \
                                  "LIMIT 1".format(self.id_indicator, partition_sqldate,self.id_variable, cursor[0], self.id_grid)

                    cursor_date = getSql(sql_ch_date, conn)
                    print(sql_ch_date)
                    print(cursor_date.rowcount)
                    if cursor_date.rowcount == 0:
                        print("Existing and resume mode : next.")
                        continue
                sqlDropPartition = "DROP TABLE annumenv_part_indicators.indicators_{0}_{1}".format(self.id_indicator, partition_sqldate)
                executeSql(sqlDropPartition,conn)

            sqlCreatePartition = "CREATE TABLE annumenv_part_indicators.indicators_{0}_{1} " \
                                 "PARTITION OF annumenv_part_indicators.indicators_{0} FOR VALUES IN ('{2}')".format(
                self.id_indicator, partition_sqldate, cursor[0].strftime('%Y-%m-%d'),
            )
            executeSql(sqlCreatePartition, conn)
            if self.trow['turbo_compute'].lower() == "true":
                executeSql(sqlInsertResults.format(
                    self.id_grid, self.id_variable, cursor[0], self.id_indicator,
                    self.sqlModeOrders[self.trow['str_mode']], self.sqlModeOrders[self.trow['num_mode']],
                    conditionVariable
                ), conn)
            else:
                sqlGetMinMaxIdCells = "SELECT min(id_cell), max(id_cell) FROM annumenv.cells WHERE id_grid = {0}".format(self.id_grid)
                id_cellExtr = getSql(sqlGetMinMaxIdCells,conn).fetchone()
                id_cellMin = id_cellExtr[0]
                id_cellMax = id_cellExtr[1]
                #self.step = 10000

                print(self.step)
                for id_cell in range(id_cellMin, id_cellMax, self.step):
                    t1 = datetime.now()
                    id_cellMaxLoop = (id_cell+self.step-1) if  (id_cell+self.step-1) < id_cellMax else id_cellMax
                    print(str(round((id_cell-id_cellMin)/(id_cellMax-id_cellMin)*100,2))
                        +" - "+str(round((id_cellMaxLoop - id_cellMin) / (id_cellMax - id_cellMin) * 100, 2))
                        +"% ("+str(id_cell-id_cellMin)+":"+str(id_cellMax-id_cellMin)+")")

                    executeSql(sqlInsertResults.format(
                              self.id_grid, self.id_variable, cursor[0], self.id_indicator,
                              self.sqlModeOrders[self.trow['str_mode']], self.sqlModeOrders[self.trow['num_mode']],
                             id_cell,id_cellMaxLoop,conditionVariable

                          ), conn)
                    print(datetime.now()-t1)

                sqlUpdate = "INSERT INTO annumenv.updates(date_result,indicators_update, date_real, feeduser) VALUES('{0}',{1}, now(), '{2}')".format(
                    date_ref, '\'{' + str(self.id_indicator) + '}\'::bigint[]', prepareStr_sql(user))
                executeSql(sqlUpdate, conn)

                #executeSql("REINDEX TABLE annumenv.indicator_results", conn)
                #executeSql("REINDEX TABLE annumenv.cells", conn)