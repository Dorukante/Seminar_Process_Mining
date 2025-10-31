from datetime import datetime
import time
from pathlib import Path

from promg import DatabaseConnection
from promg import Configuration
from promg import Performance
from promg.modules.data_importer import Importer as _PromgImporter
from promg.data_managers.semantic_header import ConstructedNodes as _PromgConstructedNodes

from main_functionalities import clear_db, load_data, transform_data, build_tasks, \
    print_statistics, add_actor_behavior, extract_decomposed_performance
from analysis_configuration import AnalysisConfiguration

analysis_config = AnalysisConfiguration()


def _delete_log_grouped_by_labels_with_retry(self, file_name):
    """
    Retry deletion of temporary CSV files to tolerate Neo4j keeping locks on Windows.
    """
    path = Path(self.get_import_directory(), file_name)
    if not path.exists():
        return

    for attempt in range(5):
        try:
            path.unlink()
            return
        except PermissionError:
            time.sleep(0.2 * (attempt + 1))

    try:
        path.unlink()
    except PermissionError:
        print(f"Warning: could not delete temporary import file {path}.")


_PromgImporter._delete_log_grouped_by_labels = _delete_log_grouped_by_labels_with_retry


def _get_df_ti_label(self):
    """
    Provide the DF_TI label used by task-instance relations (needed for handover classification).
    """
    return f"DF_TI_{self.type}"


if not hasattr(_PromgConstructedNodes, "get_df_ti_label"):
    _PromgConstructedNodes.get_df_ti_label = _get_df_ti_label

# step for adding actor behavior to case df-edges in the graph
step_add_actor_behavior = True

# step for extracting performance of case df-edges decomposed by actor behavior
# can only be turned on if the above step has run
step_extract_decomposed_performance = True


def main(config,
         step_clear_db=True,
         step_populate_graph=True,
         step_build_tasks=True,
         ) -> None:
    """
    Main function, read all the logs, clear and create the graph, perform checks
    @return: None
    """
    print("Started at =", datetime.now().strftime("%H:%M:%S"))

    db_connection = DatabaseConnection.set_up_connection(config=config)
    performance = Performance.set_up_performance(config=config)

    if step_clear_db:
        clear_db(db_connection)

    if step_populate_graph:
        load_data(db_connection=db_connection,
                  config=config)
        transform_data(db_connection=db_connection,
                       config=config)

    if step_build_tasks:
        build_tasks(db_connection=db_connection, config=config)

    if step_add_actor_behavior:
        add_actor_behavior(db_connection=db_connection, config=config, analysis_config=analysis_config)

    if step_extract_decomposed_performance:
        extract_decomposed_performance(db_connection=db_connection, config=config,
                                       analysis_config=analysis_config)

    performance.finish_and_save()
    print_statistics(db_connection)

    db_connection.close_connection()


if __name__ == "__main__":
    main(config=Configuration.init_conf_with_config_file(),
         step_clear_db=True,
         step_populate_graph=True,
         step_build_tasks=True)
