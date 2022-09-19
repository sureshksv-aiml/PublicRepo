#!flask/bin/python
# run : python C:\Office_Learning\Deephaven\Rest\PowerBi_rest_main.py
import os
from flask import Flask, request, jsonify, render_template, Response,send_file
import pandas as pd
from io import BytesIO
import numpy as np
import pyarrow.parquet as pq
from pydeephaven import Session

#from deephaven import pandas as dhpd





def load_data():
    print("load data")
    #bond_df = pd.read_csv('bond_sample_10000.csv')
    


app = Flask(__name__)

@app.route('/isAlive')
def index():
    return "true"

@app.route('/')
def home():
    return "You reached PowerBI rest service"

@app.route('/test', methods=['GET'])
def test():
    print("In getBondsJson")
    df = pd.read_csv(r"\\wsl.localhost\Ubuntu\home\suresh\deephaven-deployment\data\exportFiles\test\bond_px_sample_20.csv")
    return Response(df.to_json(orient="records"), mimetype='application/json')


@app.route('/getModelParquet', methods=['GET'])
def getModelParquet():
    print("In getModelParquet")
    return send_file(r"\\wsl.localhost\Ubuntu\home\suresh\deephaven-deployment\data\exportFiles\bond_px_view.parquet",as_attachment=True, attachment_filename="bond_px_view.parquet")

@app.route('/getModelParquetToJson', methods=['GET'])
def getModelParquetToJson():
    print("In getModelParquetToJson")
    df = pd.read_parquet(r"\\wsl.localhost\Ubuntu\home\suresh\deephaven-deployment\data\exportFiles\ref_data_px_t_minus1_view.parquet", engine='pyarrow')
    json_data = df.head(50000).to_json(orient="records")
    encoded_json = json_data.encode('utf-8')
    buf = BytesIO(encoded_json)
    return send_file(buf, mimetype='application/json', as_attachment=True, conditional=True, attachment_filename="modelParquetToJson")

    #return Response(df.head(10000).to_json(orient="records"), mimetype='application/json')

@app.route('/getDataFromDeephaven', methods=['GET'])
def getDataFromDeephaven():
    print("In getDataFromDeephaven")
    print("Before Getting Session")
    session = Session()  # assuming Deephaven Community Edition is running locally with the default configuration
    print("After Getting Session")
    #print("Existing Tables are " + str(session.tables))
    #dh_table = session.open_table("bond_px_stream_hist_filtered")
    dh_table1 = session.open_table("kafka_bond_px_intra_day")
    dh_table_filtered1 = dh_table1.view(formulas = ["CUSIP", "TRADED_PAR", "PX","YLD"])
    dh_table2 = session.open_table("ref_data")
    dh_table = dh_table_filtered1.join(dh_table2,on=["CUSIP"])
    dh_table_df = dh_table.snapshot().to_pandas()
    print("Opened deephaven Table Converted to pandas dataframe")
    return Response(dh_table_df.to_json(orient="records"), mimetype='application/json')



if __name__ == '__main__':
    app.run(port=80,host='127.0.0.1')





