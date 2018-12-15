import falcon
import mysql.connector
import json
from waitress import serve
from falcon_cors import CORS
from falcon.http_status import HTTPStatus

# WARNING this code is obviously not production worthy



def get_cursor():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="test123",
        db="dv2018",
    )
    cursor = db.cursor()

cors = CORS(allow_all_origins=True,
            allow_all_headers=True,
            allow_all_methods=True)

def extract_data(req):

    arguments = ['Collection1', 'Collection2', 'Type1', 'Type2', 'Year', 'Country', 'Limit']
    result = {}
    for argument in arguments:
        value = req.get_param(argument)
        if value is not None:
            result[argument] = value
    return result

class WorldMapResource:
    def on_get(self, req, resp):
        db = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="test123",
            db="dv2018",
        )
        cursor = db.cursor()
        worldmap_query = '''SELECT Country, Amount, Unit FROM {Collection1} WHERE Year={Year}'''
        params = extract_data(req)
        print(extract_data(req))
        full_query = worldmap_query.format(**params)
        print(full_query)
        cursor.execute(full_query)
        query_result = cursor.fetchall()
        result = []
        for row in query_result:
            row_result = {
                'Label': row[0],
                'Amount': row[1],
                'Unit': row[2]
            }
            result.append(row_result)
        resp.body = json.dumps(result)


class ScatterPlotResource:
    def on_get(self, req, resp):
        db = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="test123",
            db="dv2018",
        )
        cursor = db.cursor()
        scatterplot_query = """SELECT {Collection1}.Country, {Collection1}.amount, {Collection1}.Unit, {Collection2}.amount, {Collection2}.Unit 
                              FROM {Collection1} JOIN {Collection2}
                              ON {Collection1}.Country={Collection2}.Country
                              AND {Collection1}.Year={Collection2}.Year
                              WHERE {Collection1}.Year = {Year}
                              """
        full_query = scatterplot_query.format(**extract_data(req))
        cursor.execute(full_query)
        query_result = cursor.fetchall()
        result = {'x-axis': [], 'y-axis': [], 'x-label': [], 'y-label': []}
        for row in query_result:
            result['x-axis'].append(row[1])
            result['y-axis'].append(row[3])
            result['x-label'].append(row[2])
            result['y-label'].append(row[4])
        resp.body = json.dumps(result)

class PieChartResource:
    def on_get(self, req, resp):
        db = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="test123",
            db="dv2018",
        )

        cursor = db.cursor()
        partial_query = '''SELECT Country, Amount, Unit FROM {Collection1} WHERE Year={Year} ORDER BY Amount DESC'''
        params = extract_data(req)
        others_query = None
        if params['Limit']:
            piechart_query = partial_query + ''' Limit {Limit}'''
            others_query = '''SELECT SUM(sub.Amount) FROM (SELECT Amount FROM {Collection1} WHERE Year={Year} ORDER BY Amount DESC LIMIT 18446744073709551615 OFFSET {Limit}) sub'''
        else:
            piechart_query = partial_query
        params = extract_data(req)
        full_query = piechart_query.format(**params)

        cursor.execute(full_query)
        query_result = cursor.fetchall()
        result = []
        unit = ''
        for row in query_result:
            row_result = {
                'Label': row[0],
                'Amount': row[1],
                'Unit': row[2]
            }
            unit = row[2]
            result.append(row_result)
        if others_query:
            others_query = others_query.format(**params)
            cursor.execute(others_query)
            others_result = cursor.fetchall()
            for row in others_result:
                result.append({
                    'Label': 'Other',
                    'Amount': str(row[0]),
                    'Unit': unit
                })
        resp.body = json.dumps(result)


api = falcon.API(middleware=[cors.middleware ])
api.add_route('/worldMap', WorldMapResource())
api.add_route('/scatterPlot', ScatterPlotResource())
api.add_route('/pieChart', PieChartResource())
serve(api, listen='*:8080')