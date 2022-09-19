from deephaven.appmode import ApplicationState, get_app_state
from deephaven import read_csv,empty_table
from typing import Callable
import time,datetime
from pytz import timezone


def create_views():
    ref_data_px_t_minus1_view = empty_table(0)
    try:
        start = time.perf_counter()
        start_time_str = datetime.datetime.now(timezone('EST')).strftime("%H:%M:%S EST")
        ref_data_px_t_minus1_view = ref_data.natural_join(table=px_t_minus1, on=["CUSIP"])
        #Following will result non matching  bond_ref entries data as null
        #bondRef_px_view = bond_px_yday.natural_join(table=bonds_refData, on=["CUSIP"])

        end = time.perf_counter()
        end_time_str = datetime.datetime.now(timezone('EST')).strftime("%H:%M:%S EST")
        timeElapsed=str(datetime.timedelta(seconds=round(end - start)))
        table_writer.write_row("Model ref_data_px_t_minus1_view generation","Success",ref_data_px_t_minus1_view.size, start_time_str,end_time_str,timeElapsed)
    except Exception as exception:
        print("Error: %s!\n\n" % exception)
        table_writer.write_row("Model ref_data_px_t_minus1_view generation","Failure",ref_data_px_t_minus1_view.size, start_time_str,end_time_str,timeElapsed)
    return ref_data_px_t_minus1_view
    
    
ref_data_px_t_minus1_view = create_views()


def start(app: ApplicationState):
    
    print("Model Generation - Binding to Tables Completed")

def initialize(func: Callable[[ApplicationState], None]):
    print("Model Generation - Init - Start")
    app = get_app_state()
    print("Model Generation - Init - Got App State")
    func(app)
    print("Model Generation - Init - End")
    print('***' * 20)
initialize(start)