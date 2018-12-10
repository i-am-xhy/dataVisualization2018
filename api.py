import falcon
import pymysql.cursors
import json

# WARNING this code is obviously not production worthy

db = pymysql.connect(
    host="localhost",
    user="root",
    passwd="test123",
    db="dv2018",
)

cursor = db.cursor()

def extract_data(req):

    arguments = ['Collection1', 'Collection2', 'Type1', 'Type2', 'Year', 'Country']
    result = {}
    for argument in arguments:
        value = req.get_param(argument)
        if value is not None:
            result[argument] = value
    return result

class WorldMapResource:
    def on_get(self, req, resp):
        worldmap_query = '''SELECT Country, Amount FROM {Collection1} WHERE Year={Year}'''
        print(extract_data(req))
        full_query = worldmap_query.format(**extract_data(req))
        print(full_query)
        cursor.execute(full_query)
        query_result = cursor.fetchall()
        result = []
        for row in query_result:
            row_result = {
                'Label': row[0],
                'Amount': row[1]
            }
            result.append(row_result)
        resp.body = json.dumps(result)


class ScatterPlotResource:
    def on_get(self, req, resp):
        scatterplot_query = """SELECT {Collection1}.Country, {Collection1}.amount, {Collection2}.amount 
                              FROM {Collection1} JOIN {Collection2}
                              ON {Collection1}.Country={Collection2}.Country
                              AND {Collection1}.Year={Collection2}.Year
                              WHERE {Collection1}.Year = {Year}
                              """
        full_query = scatterplot_query.format(**extract_data(req))
        cursor.execute(full_query)
        query_result = cursor.fetchall()
        result = {'x-axis': [], 'y-axis': []}
        for row in query_result:
            result['x-axis'].append(row[1])
            result['y-axis'].append(row[2])
        resp.body = json.dumps(result)

class PieChartResource:
    piechart_query = """"""


api = falcon.API()
api.add_route('/worldMap', WorldMapResource())
api.add_route('/scatterPlot', ScatterPlotResource())
api.add_route('/pieChart', PieChartResource())