from datetime import datetime
from promg import DatabaseConnection
from promg import Configuration
from promg import Performance

from main_functionalities import clear_db, load_data, transform_data, build_tasks, \
    print_statistics, add_actor_behavior, extract_decomposed_performance
from analysis_configuration import AnalysisConfiguration

analysis_config = AnalysisConfiguration()

# step for adding actor behavior to case df-edges in the graph
step_add_actor_behavior = False

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
         step_clear_db=False,
         step_populate_graph=False,
         step_build_tasks=False)
