import pygeoif
import csv
from datetime import datetime, timezone

epsg = [
    [4326, 'GEOGCS["GCS_WGS_1984"'],
    [2154, 'PROJCS["Lambert_Conformal_Conic"'],
    [2154, 'PROJCS["RGF_1993_Lambert_93"'],
    [2154, 'PROJCS["RGF93_Lambert_93"']
]
dateFormats = {
    "%b %d %Y %H:%M:%S":"MMM DD YYYY hh:mm:ss",
    "%d %b %Y %H:%M:%S":"DD MMM YYYY hh:mm:ss",
    "%d/%m/%Y %H:%M:%S":"DD/MM/YYYY hh:mm:ss",
    "%d-%m-%Y %H:%M:%S":"DD-MM-YYYY hh:mm:ss",
    "%m-%d-%Y %H:%M:%S":"MM-DD-YYYY hh:mm:ss",
    "%m/%d/%Y %H:%M:%S":"MM/DD/YYYY hh:mm:ss",
    "%Y-%d-%m %H:%M:%S":"YYYY/DD/MM hh:mm:ss",
    "%Y/%d/%m %H:%M:%S":"YYYY/DD/MM hh:mm:ss",
    "%Y-%m-%d %H:%M:%S":"YYYY-MM-DD hh:mm:ss",
    "%Y/%m/%d %H:%M:%S":"YYYY/MM/DD hh:mm:ss",
    "%d %m %Y %H:%M:%S":"DD MM YYYY hh:mm:ss",
    "%m %d %Y %H:%M:%S":"MM DD YYYY hh:mm:ss",
    "%Y %d %m %H:%M:%S":"YYYY DD MM hh:mm:ss",
    "%Y %m %d %H:%M:%S":"YYYY MM DD hh:mm:ss",
    "%d.%m.%Y %H:%M:%S":"DD.MM.YYYY hh:mm:ss",
    "%m.%d.%Y %H:%M:%S":"MM.DD.YYYY hh:mm:ss",
    "%Y.%d.%m %H:%M:%S":"YYYY.DD.MM hh:mm:ss",
    "%Y.%m.%d %H:%M:%S":"YYYY.MM.DD hh:mm:ss","%d %b %Y":"DD MMM YYYY",
    "%d/%m/%Y":"DD/MM/YYYY",
    "%d-%m-%Y":"DD-MM-YYYY",
    "%m-%d-%Y":"MM-DD-YYYY",
    "%m/%d/%Y":"MM/DD/YYYY",
    "%Y-%d-%m":"YYYY/DD/MM",
    "%Y/%d/%m":"YYYY/DD/MM",
    "%Y-%m-%d":"YYYY-MM-DD",
    "%Y/%m/%d":"YYYY/MM/DD",
    "%d %m %Y":"DD MM YYYY",
    "%m %d %Y":"MM DD YYYY",
    "%Y %d %m":"YYYY DD MM",
    "%Y %m %d":"YYYY MM DD",
    "%d.%m.%Y":"DD.MM.YYYY",
    "%m.%d.%Y":"MM.DD.YYYY",
    "%Y.%d.%m":"YYYY.DD.MM",
    "%Y.%m.%d":"YYYY.MM.DD"
}
booleans = [
    "oui", "yes","true","o","y","1","t","non","no","false","n","0","f"
]
null_values={"nc","na","n"}

def get_wkt(shape):
    try:
        geom_type = shape.__geo_interface__["type"]
    except Exception as e:
        print(e)
        return None
    if(geom_type.lower()=="polygon"):
        return pygeoif.Polygon(pygeoif.geometry.as_shape(shape)).wkt
    if (geom_type.lower() == "point"):
        return pygeoif.Point(pygeoif.geometry.as_shape(shape)).wkt
    if (geom_type.lower() == "linestring"):
        return pygeoif.LineString(pygeoif.geometry.as_shape(shape)).wkt
    if (geom_type.lower() == "linearring"):
        return pygeoif.LinearRing(pygeoif.geometry.as_shape(shape)).wkt
    if(geom_type.lower()=="multipolygon"):
        return pygeoif.MultiPolygon(pygeoif.geometry.as_shape(shape)).wkt
    if (geom_type.lower() == "multipoint"):
        return pygeoif.MultiPoint(pygeoif.geometry.as_shape(shape)).wkt
    if (geom_type.lower() == "multilinestring"):
        return pygeoif.MultiLineString(pygeoif.geometry.as_shape(shape)).wkt


def testBoolean(list):
    for elt in list:
        if elt.lower() not in booleans:
            return False
    return True

def testInt(list):
    for elt in list:
        if elt.startswith("0") and not "." not in elt:
            return False
        try:
            if "." in elt or "," in elt:
                return False
            int(elt)
        except ValueError:
            return False
    return True

def testNumeric(list):
    for elt in list:
        if elt.startswith("0") and not "." not in elt:
            return False
        try:
            float(elt.replace(",","."))
        except ValueError:
            return False
    return True

def testDateF(list, formatDate):
    for elt in list:
        try:
            datetime.strptime(elt, formatDate)
        except ValueError:
            return False
    return True

def testDateFu(elt, formatDate):
    try:
        datetime.strptime(elt, formatDate)
    except ValueError:
        return False
    return True

def testDate(list):
    headers = dateFormats.keys()
    for elt in headers:
        if testDateF(list, elt):
            return True
    return False

def bestDateFormat(list):
    headers = dateFormats.keys()
    for elt in headers:
        if testDate(list, elt):
            return elt
    return None

def bestDateFormat_u(elt1):
    list = [elt1]
    headers = dateFormats.keys()
    for elt in headers:
        if testDateF(list, elt):
            return elt
    return None

def get_init_srid(initfilepath):
    filepath = str(initfilepath)[:-4]+".prj"
    proj = open(filepath, "r").read()
    for elt in epsg:
        if(proj.lower().startswith(elt[1].lower())): return elt[0]

def guessCsvColType(filepath, helt, LIM_CSV_HG=100000):
    set = list()
    with open(filepath, newline='') as csvfile:
        csvR = csv.DictReader(csvfile, delimiter=';', quotechar='"')
        i=0
        for elt in csvR:
            if elt[helt] and len(elt[helt])>0:
                set.append(elt[helt])
                i+=1
            if i>=LIM_CSV_HG:
                break;
        if len(set)==0 :
            return "text"
        if testBoolean(set):
            return "boolean"
        if testDate(set):
            return "date"
        if testInt(set):
            return "bigint"
        if testNumeric(set):
            return "numeric"
    return 'text'


def guessCsvDateFormat(filepath, helt, LIM_CSV_HG=100):
    set = list()
    with open(filepath, newline='') as csvfile:
        csvR = csv.DictReader(csvfile, delimiter=';', quotechar='"')
        i=0
        for elt in csvR:
            if elt[helt] and len(str(elt[helt]))>0:
                set.append(elt[helt])
                i+=1
                if i>=LIM_CSV_HG:
                    break;
        return bestDateFormat(set)
    return None


def prepare_data_shp(data_type, data_value):
    if not data_value or data_value==None or not bool(data_value) or len(str(data_value))==0:
        return "NULL"
    elif(data_type=="text"):
        return "'"+data_value.replace("'","''")+"'::text"
    elif(data_type=="bigint"):
        if data_value in null_values:
            return 'NULL'
        return str(data_value)+"::bigint"
    elif (data_type == "timestamp"):
        if data_value in null_values:
            return 'NULL'
        return "(to_date('" + str(data_value).replace(",", ".") + "', 'YYYY-MM-DD')::date)::timestamp"
    elif(data_type=="date"):
        if data_value in null_values:
            return 'NULL'
        return  "to_date('"+str(data_value).replace(",",".")+"', 'YYYY-MM-DD')::date"
    elif(data_type=="numeric"):
        if data_value in null_values:
            return 'NULL'
        return str(data_value)+"::numeric"
    elif(data_type=="boolean"):
        if data_value in null_values:
            return 'NULL'
        if str(data_value).lower().startswith("y") or str(data_value).lower().startswith("o") or str(data_value).lower().startswith("1"):
            return "true::boolean"
        elif str(data_value).lower().startswith("n") or str(data_value).lower().startswith("0"):
            return "false::boolean"
        else:
            return "null"
        return data_value+"::bigint"
    else:
        return data_value;

def prepare_timestamp(data_value, date_format):
    if data_value in null_values:
        return 'NULL'
    return "(to_date('" + str(data_value) + "', '" + date_format + "')::date)::timestamp"

def prepare_data_csv(data_type, data_value):
    if not data_value or data_value==None or not bool(data_value) or len(str(data_value))==0:
        return "NULL"

    if(data_type=="text"):
        return "'"+data_value.replace("'","''")+"'::text"
    elif(data_type=="bigint"):
        if data_value in null_values:
            return 'NULL'
        return str(data_value)+"::bigint"
    elif (data_type == "timestamp"):
        if data_value in null_values:
            return 'NULL'
        return "(to_date('" + str(data_value).replace(",", ".") + "', 'YYYYMMDD')::date)::timestamp"
    elif(data_type=="date"):
        if data_value in null_values:
            return 'NULL'
        return  "to_date('"+str(data_value)+"', '"+dateFormats[bestDateFormat_u(data_value)]+"')::date"
    elif(data_type=="numeric"):
        if data_value in null_values:
            return 'NULL'
        return str(data_value)+"::numeric"
    elif(data_type=="boolean"):
        if data_value in null_values:
            return 'NULL'
        if str(data_value).lower().startswith("y") or str(data_value).lower().startswith("o") or str(data_value).lower().startswith("1"):
            return "true::boolean"
        elif str(data_value).lower().startswith("n") or str(data_value).lower().startswith("0"):
            return "false::boolean"
        else:
            return "null"
        return data_value+"::bigint"
    else:
        return data_value;
