import psycopg2
import shapefile

from scripts.psql_fun import *
from scripts.types_fun import *


def create_table(trow, columns, deffile, conn):
    sqlOrder = "SELECT count(*) FROM information_schema.schemata WHERE schema_name = '%s'" % (trow['table_schema'])
    cur = conn.cursor()
    cur.execute(sqlOrder)
    scount = cur.fetchone()
    if scount[0] == 0:
        sqlOrder = 'CREATE SCHEMA "%s"' % (trow['table_schema'])
        executeSql(sqlOrder, conn)

    if trow['append'].lower() == "false":
        sqlOrder = 'DROP TABLE IF EXISTS "%s"."%s" CASCADE' % (trow['table_schema'], trow['table_name'])
        executeSql(sqlOrder, conn)
    else:
        return

    sqlOrder = 'CREATE TABLE "%s"."%s" (' % (trow['table_schema'], trow['table_name'])
    i = 0
    if columns and len(columns) > 0:
        for col in columns:
            col_descr = '"%s" %s' % (col["column_name"], deffile["types"][col["origin_column"]])
            if i == 0:
                sqlOrder += col_descr
            else:
                sqlOrder += "," + col_descr
            i += 1
    if trow["origin_file"].lower().endswith("shp"):
        sqlOrder += ",geom geometry" if i>0 else "geom geometry"
    pkeys = []
    if columns and len(columns) > 0:
        for col in columns:
            if col["primary_key"].lower() == "true":
                pkeys.append(col["column_name"])
    if len(pkeys) > 0:
        sqlOrder += ", PRIMARY KEY ("
        for elt in pkeys:
            if sqlOrder.endswith("("):
                sqlOrder += '"' + elt + '"'
            else:
                sqlOrder += ',"' + elt + '"'
        sqlOrder += ")"
    sqlOrder += ")"
    executeSql(sqlOrder, conn)


def executeSqlU(orderSql, conn):
    cur = conn.cursor()
    try:
        cur.execute(orderSql)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def executeSql(orderSql, conn):
    if isinstance(orderSql, list):
        for etl in orderSql:
            executeSqlU(etl, conn)
    else:
        executeSqlU(orderSql, conn)

def getSqlColType(schema, table, column, conn):
    return getSql("SELECT data_type FROM information_schema.columns "\
            "WHERE table_schema = '{0}' AND table_name = '{1}' AND column_name = '{2}'".format(schema, table, column),
           conn).fetchone()[0]


def getSql(request, conn):
    cur = conn.cursor()
    cur.execute(request)
    return cur


def deadlock(columns, r):
    for elt in columns:
        if elt["condition"] and len(str(elt["condition"])) > 0:
            if r[elt["origin_column"]] != elt["condition"] and str(r[elt["origin_column"]]) != str(elt["condition"]):
                return True
    return False

def insert_rows(trow, columns, file_def, conn, LIMIT_SIZE=10):

    print("Prepare SQL 4 SHP : {0}.{1}".format(trow['table_schema'], trow['table_name']))
    sqlOrder_0 = 'INSERT INTO "{0}"."{1}" VALUES '.format(trow['table_schema'], trow['table_name'])
    sqlOrder = sqlOrder_0
    k = 0
    l = 0
    count = 0;
    orders = []

    typeValCol = dict()
    for col in columns:
        typeValCol[col['origin_column']] = getSqlColType(trow['table_schema'], trow['table_name'], col['column_name'], conn) if trow['append'].lower() == "true" else file_def["types"][col["origin_column"]]

    if trow["origin_file"].lower().endswith("csv"):
        if not columns or len(columns) == 0:
            return
        with open(trow["origin_file"], newline='') as csvfile:
            csvR = csv.DictReader(csvfile, delimiter=';', quotechar='"')
            i = 0
            for r in csvR:
                l +=1
                if l%100000 == 0:
                    print("Ligne à intégrer {0}...".format(l))
                if deadlock(columns, r):
                    continue
                count += 1
                sqlOrder_sub = ""
                for col in columns:
                    col_val = prepare_data_csv(typeValCol[col['origin_column']], r[col["origin_column"]])
                    if (sqlOrder_sub == ""):
                        sqlOrder_sub = "(" + col_val
                    else:
                        sqlOrder_sub += "," + col_val
                sqlOrder_sub += ")"
                if k == 0:
                    sqlOrder += sqlOrder_sub
                else:
                    sqlOrder += "," + sqlOrder_sub
                sqlOrder_sub += ")"
                if k >= LIMIT_SIZE - 1:
                    executeSql(sqlOrder, conn)
                    print("... {0}.{1} : {2}".format(trow['table_schema'], trow['table_name'], count))
                    sqlOrder = sqlOrder_0
                    k = 0
                else:
                    k += 1
            if not sqlOrder.endswith(" VALUES "):
                executeSql(sqlOrder, conn)
            print("OK : {0}.{1} : total {2}".format(trow['table_schema'], trow['table_name'], count))

    if trow["origin_file"].lower().endswith("shp"):
        srid_init = get_init_srid(trow['origin_file'])
        shape = shapefile.Reader(trow['origin_file'])
        field_names = [field[0] for field in shape.fields[1:]]
        for r in shape.shapeRecords():
            l += 1
            if l % 100000 == 0:
                print("Ligne à intégrer {0}...".format(l))
            if deadlock(columns, r):
                continue
            count += 1
            atr = dict(zip(field_names, r.record))
            wkt = get_wkt(r.shape)
            if wkt is None:
                continue
            sqlOrder_sub = ""
            if columns and len(columns) > 0:
                for col in columns:
                    col_val = prepare_data_shp(typeValCol[col['origin_column']], atr[col["origin_column"]])
                    if (sqlOrder_sub == ""):
                        sqlOrder_sub = "(" + col_val
                    else:
                        sqlOrder_sub += "," + col_val
            sqlGeom = "ST_MakeValid(ST_Transform(ST_setSrid(ST_GeomFromText('{0}')::geometry,{1}),{2}))".format(wkt,srid_init,trow['dest_srid']);
            sqlOrder_sub += "," + sqlGeom if sqlOrder_sub != "" else "(" +sqlGeom
            sqlOrder_sub += ")"
            if k == 0:
                sqlOrder += sqlOrder_sub
            # k += 1
            else:
                sqlOrder += "," + sqlOrder_sub
            if k >= LIMIT_SIZE - 1:
                executeSql(sqlOrder, conn)
                print("... {0}.{1} : {2}".format(trow['table_schema'], trow['table_name'], count))
                sqlOrder = sqlOrder_0
                k = 0
            else:
                k += 1
        if not sqlOrder.endswith(" VALUES "):
            executeSql(sqlOrder, conn)
            executeSql(
                'UPDATE "{0}"."{1}" SET geom = st_makevalid(geom)'.format(trow['table_schema'], trow['table_name']),
                conn)
        print("OK : {0}.{1} : total {2}".format(trow['table_schema'], trow['table_name'], count))
    return orders


def set_indexes(trow, columns, conn):
    if trow['append'].lower() == "true":
        return
    print("Creating indexes... " + trow["table_name"])
    if trow["origin_file"].lower().endswith("shp"):
        sqlOrder = 'CREATE INDEX "{0}_{1}_idx_geom" ON "{0}"."{1}" USING GIST(geom)'.format(trow['table_schema'],
                                                                                            trow['table_name'])
        executeSql(sqlOrder, conn)
    for col in columns:
        if col['index'].lower() == 'true':
            sqlOrder = 'CREATE INDEX ON "{0}"."{1}"("{2}")'.format(trow['table_schema'], trow['table_name'],
                                                                   col['column_name'])
            executeSql(sqlOrder, conn)


def set_geoms(trow, columns, conn):
    if trow["origin_file"].lower().endswith("shp"):
        return 'NULL'
    print("Updating geometry references for " + trow['table_name'])
    sqlCreateCol = 'ALTER TABLE "{0}"."{1}" ADD COLUMN geom geometry'
    sqlUpdateCol = 'UPDATE "{0}"."{1}" SET geom = st_makevalid(ST_TRANSFORM(a1.geom,{2})) FROM "{3}"."{4}" a1 WHERE "{1}"."{5}"::text=a1."{6}"::text AND "{1}".geom IS NULL'
    var_x = None
    var_y = None

    if testInt(trow["proj_init_ifproj"]):
        sqlUpdateCol_xy = 'UPDATE "{0}"."{1}" SET geom = st_MakeValid(ST_TRANSFORM(ST_SetSRID(ST_MakePoint("{2}"::numeric,"{3}"::numeric),{4}),{5})) WHERE "{1}".geom IS NULL'
    else:
        sqlUpdateCol_xy = 'UPDATE "{0}"."{1}" SET geom = st_MakeValid(ST_TRANSFORM(ST_SetSRID(ST_MakePoint("{2}"::numeric,"{3}"::numeric),"'+trow["proj_init_ifproj"]+'"),{5})) WHERE "{1}".geom IS NULL'
    getNbCol = 'WITH NN as (SELECT count(*)::numeric as cNN FROM "{0}"."{1}" WHERE geom IS NOT NULL) ' \
               'SELECT cNN/count(*)::numeric FROM "{0}"."{1}", NN GROUP BY NN.cNN'

    for geom in [col for col in columns if (not col['geom_join_on'] is None and len(col['geom_join_on']) != 0)]:
        if trow['append'].lower() == "false":
            executeSql(sqlCreateCol.format(trow['table_schema'], trow['table_name']), conn)
        executeSql(sqlUpdateCol.format(trow['table_schema'], trow['table_name'], trow['dest_srid'],
                                       geom["ref_geom_table_schema"], geom["ref_geom_table_name"], geom['column_name'],
                                       geom["geom_join_on"]), conn)
        if trow['append'].lower() == "false":
            executeSql('CREATE INDEX ON "{0}"."{1}" USING GIST(geom)'.format(trow['table_schema'], trow['table_name']),
                   conn)
        return getSql(getNbCol.format(trow['table_schema'], trow['table_name']), conn).fetchone()[0]

    for geom in [col for col in columns if
                 (not col['coordinates_ifproj'] is None and col['coordinates_ifproj'] == 'x')]:
        var_x = geom['column_name']
        break

    for geom in [col for col in columns if
                 (not col['coordinates_ifproj'] is None and col['coordinates_ifproj'] == 'y')]:
        var_y = geom['column_name']
        break

    if var_x and var_y:
        if trow['append'].lower() == "false":
            executeSql(sqlCreateCol.format(trow['table_schema'], trow['table_name']), conn)
        executeSql('CREATE INDEX ON "{0}"."{1}" USING GIST(geom)'.format(trow['table_schema'], trow['table_name']),
                   conn)
        #if trow['append'].lower() == "false":
        executeSql(sqlUpdateCol_xy.format(trow['table_schema'], trow['table_name'], var_x, var_y, trow["proj_init_ifproj"], trow["dest_srid"]),
                   conn)
        return getSql(getNbCol.format(trow['table_schema'], trow['table_name']), conn).fetchone()[0]

    return 'NULL'


def check_table(schema, table, conn):
    sqlOrder = "SELECT count(*) FROM pg_tables WHERE schemaname = '{0}' AND tablename = '{1}'".format(schema, table)
    cur = conn.cursor()
    cur.execute(sqlOrder)
    if cur.fetchone()[0] == 0:
        return False
    return True


def prepareStr_sql(valStr):
    return str(valStr).replace("'", "''")


def follow_tracks(trow, columns, conn, user):
    sqlOrder = "SELECT count(*) FROM pg_tables WHERE schemaname = '%s' AND tablename = 'table_tracks'" % (
    trow['table_schema'])
    cur = conn.cursor()
    cur.execute(sqlOrder)
    if cur.fetchone()[0] == 0:
        track_table_sql = 'CREATE TABLE "{0}"."table_tracks" (table_schema text, table_name text, append boolean,	origin_file text, dest_srid	int, description text, feeduser text, feeddate timestamp with time zone, geom_score numeric)'.format(
            trow['table_schema'])
        executeSql(track_table_sql, conn)
    track_table_sql = 'INSERT INTO "{0}"."table_tracks" VALUES(\'{0}\', \'{1}\', \'{2}\'::boolean,\'{3}\', {4}, \'{5}\', \'{6}\', \'{7}\', {8})'.format(
        trow['table_schema'], trow['table_name'], trow['append'], trow['origin_file'],
        prepare_data_csv('bigint', trow['dest_srid']), trow['description'].replace("'", "''"), user,
        datetime.now(timezone.utc), set_geoms(trow, columns, conn))
    executeSql(track_table_sql, conn)

    sqlOrder = "SELECT count(*) FROM pg_tables WHERE schemaname = '%s' AND tablename = 'column_tracks'" % (
    trow['table_schema'])
    cur = conn.cursor()
    cur.execute(sqlOrder)
    if cur.fetchone()[0] == 0:
        track_column = 'CREATE TABLE "{0}"."column_tracks" (table_schema text, table_name text,	column_name text, origin_column text,description text, feeduser text, feeddate date)'.format(
            trow['table_schema'])
        executeSql(track_column, conn)
    track_column = "INSERT INTO \"{0}\".\"column_tracks\" VALUES('{0}','{1}',{2},{3},{4},{5},'{6}')"
    for col in columns:
        executeSql(
            track_column.format(trow['table_schema'], trow['table_name'], prepare_data_csv('text',col["column_name"]), prepare_data_csv('text',col["origin_column"]),
                                prepare_data_csv('text',col["description"]), prepare_data_csv('text',user), datetime.now(timezone.utc)), conn)
