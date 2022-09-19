from deephaven.appmode import ApplicationState, get_app_state
from deephaven import read_csv,empty_table

from typing import Callable
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht
import time,datetime
from pytz import timezone


def init_process_status():
    column_definitions = {"Process": dht.string , "Status": dht.string, "No of Records": dht.int32, "Start": dht.string , "End": dht.string , "Time Elapsed": dht.string}
    table_writer = DynamicTableWriter(column_definitions)
    process_status = table_writer.table
    return process_status,table_writer


def load_data(file_path, label):
    loaded_data = empty_table(0)
    try:
        start = time.perf_counter()
        start_time_str = datetime.datetime.now(timezone('EST')).strftime("%H:%M:%S EST")
        loaded_data = read_csv(file_path)
        end = time.perf_counter()
        end_time_str = datetime.datetime.now(timezone('EST')).strftime("%H:%M:%S EST")
        timeElapsed=str(datetime.timedelta(seconds=round(end - start)))
        print(label + " Loaded")
        table_writer.write_row(label,"Success", loaded_data.size, start_time_str,end_time_str,timeElapsed)        
    except Exception as exception:
        print("Error: %s!\n\n" % exception)
        table_writer.write_row(label,"Failure",loaded_data.size, start_time_str,end_time_str,timeElapsed)
    return loaded_data


process_status,table_writer = init_process_status()


ref_data = load_data("/data/importFiles/bond_sample.csv","Ingestion - Bonds Refdata Ingestion")
px_t_minus1 = load_data("/data/importFiles/bond_px_sample.csv","Ingestion - Bond Price T-1 Ingestion")
px_t_minus2_and_before = load_data("/data/importFiles/bond_px_history_sample.csv","Ingestion - Bond Price T-2 and Before Ingestion")
#bond_px_history_100Mil = load_data("/data/importFiles/bond_px_dated_sample_hundredmllion.csv","Ingestion - Bond Price history 100mil Ingestion")






def start(app: ApplicationState):
    print("Ingestion - Binding to Tables Completed")

def initialize(func: Callable[[ApplicationState], None]):
    print("Ingestion - Init - Start")
    app = get_app_state()
    print("Ingestion - Init - Got App State")
    func(app)
    print("Ingestion - Init - End")
    print("Ingestion - Init - End")
    print('***'*20)
initialize(start)