from scripts.psql_fun import *

corr_types = {
    "C":"text",
    "N":"bigint",
    "D":"date",
    "F":"numeric",
    "L":"boolean"
}

def flt_tcolumns(crow,sname,tname):
    return crow['table_schema']==sname and crow['table_name']==tname

def flt_scolumns(crow,sname,tname):
    return crow['table_schema']==sname and crow['table_name']==tname

def gettablefile_def(filepath, ocols):
    if filepath.lower().endswith("shp"):
        shape = shapefile.Reader(filepath)
        shape_types = dict()
        for field in shape.fields[1:]:
            if field[0] in ocols:
                shape_types[field[0]] = corr_types[field[1]]
        return dict(types=shape_types)
    if filepath.lower().endswith("csv"):
        with open(filepath, newline='') as csvfile:
            csvR = csv.DictReader(csvfile, delimiter=';', quotechar='"')
            headers = csvR.fieldnames
            csv_types = dict()
            for elt in headers:
                if elt in ocols:
                    csv_types[elt] = guessCsvColType(filepath, elt)
            return dict(types=csv_types)

def getcolumns_tablefile(sname,tname):
    with open('config/columns.csv', newline='') as csvfile:
        creader = csv.DictReader(csvfile, delimiter=';', quotechar='"')
        return list(filter(lambda seq:flt_tcolumns(seq,sname,tname),creader)).copy()

def list_col_names(columns):
    list = []
    for elt in columns :
        list.append(elt["origin_column"])
    return list

def deal_tablefile(trow,conn,user):
    print("### "+trow["table_name"]+" : "+trow["origin_file"])
    print("Définition de la table...")
    columns = getcolumns_tablefile(trow['table_schema'],trow['table_name'])
    file_def = None
    if trow["append"].lower() == 'false':
        file_def = gettablefile_def(trow['origin_file'], list_col_names(columns))
        create_table(trow, columns, file_def, conn)
    print("Alimentation de la table...")
    insert_rows(trow, columns, file_def, conn, int(trow['limit_load_size']))
    set_indexes(trow, columns, conn)
    follow_tracks(trow, columns, conn,user)
