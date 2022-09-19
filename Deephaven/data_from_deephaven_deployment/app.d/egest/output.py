from deephaven.appmode import ApplicationState, get_app_state
from deephaven.parquet import write
from typing import Callable
import time,datetime
from pytz import timezone


def generate_output(): 
    try:
        start = time.perf_counter()
        start_time_str = datetime.datetime.now(timezone('EST')).strftime("%H:%M:%S EST")
        write(ref_data_px_t_minus1_view, "/data/exportFiles/ref_data_px_t_minus1_view.parquet")
        end = time.perf_counter()
        end_time_str = datetime.datetime.now(timezone('EST')).strftime("%H:%M:%S EST")
        timeElapsed=str(datetime.timedelta(seconds=round(end - start)))
        table_writer.write_row("BondRef_px_view Parquet file Persistance","Success",ref_data_px_t_minus1_view.size, start_time_str,end_time_str,timeElapsed)
    except Exception as exception:
        print("Error: %s!\n\n" % exception)
        table_writer.write_row("BondRef_px_view Parquet file Persistance","Failure",ref_data_px_t_minus1_view.size, start_time_str,end_time_str,timeElapsed)


generate_output()



def start(app: ApplicationState):
    print("Output - Binding to Tables Completed")

def initialize(func: Callable[[ApplicationState], None]):
    print("Output - Init - Start")
    app = get_app_state()
    print("Output - Init - Got App State")
    func(app)
    print("Output - Init - End")
    print('***' * 20)
initialize(start)