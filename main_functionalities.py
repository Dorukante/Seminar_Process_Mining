from colorama import Fore
from promg import OcedPg, SemanticHeader, DatasetDescriptions
from promg.modules.db_management import DBManagement
from promg.modules.task_identification import TaskIdentification

from modules.decomposition_actor_behavior.decomposition_actor_behavior import DecompositionActorBehavior
from modules.custom_modules.delay_analysis import PerformanceAnalyzeDelays
from modules.custom_modules.df_interactions import InferDFInteractions
from modules.custom_modules.discover_dfg import DiscoverDFG


def clear_db(db_connection):
    print(Fore.RED + 'Clearing the database.' + Fore.RESET)

    db_manager = DBManagement(db_connection)
    db_manager.clear_db(replace=True)
    db_manager.set_constraints()


def load_data(db_connection, config):
    if config.use_preprocessed_files:
        print(Fore.RED + '💾 Preloaded files are used!' + Fore.RESET)
    else:
        print(Fore.RED + '📝 Importing and creating files' + Fore.RESET)

    semantic_header = SemanticHeader.create_semantic_header(config=config)
    dataset_descriptions = DatasetDescriptions(config=config)

    oced_pg = OcedPg(database_connection=db_connection,
                     semantic_header=semantic_header,
                     dataset_descriptions=dataset_descriptions,
                     store_files=False,
                     use_sample=config.use_sample,
                     use_preprocessed_files=config.use_preprocessed_files,
                     import_directory=config.import_directory)
    oced_pg.load()


def transform_data(db_connection,
                   config):
    dataset_descriptions = DatasetDescriptions(config=config)
    semantic_header = SemanticHeader.create_semantic_header(config=config)

    oced_pg = OcedPg(database_connection=db_connection,
                     semantic_header=semantic_header,
                     dataset_descriptions=dataset_descriptions,
                     store_files=False,
                     use_sample=config.use_sample,
                     use_preprocessed_files=config.use_preprocessed_files,
                     import_directory=config.import_directory)

    oced_pg.transform()
    oced_pg.create_df_edges()


def delete_parallel_df(db_connection, config):
    semantic_header = SemanticHeader.create_semantic_header(config=config)
    print(Fore.RED + 'Inferring DF over relations between objects.' + Fore.RESET)
    print("s", semantic_header)
    infer_df_interactions = InferDFInteractions(db_connection=db_connection, semantic_header=semantic_header)
    
    if semantic_header.name == "BPIC15":
        infer_df_interactions.delete_parallel_directly_follows_derived('Application', 'Application')
        infer_df_interactions.delete_parallel_directly_follows_derived('Application', 'Resource')
        infer_df_interactions.delete_parallel_directly_follows_derived('Application', 'MonitoringResource')
        infer_df_interactions.delete_parallel_directly_follows_derived('Application', 'ResponsibleActor')
    else:
        infer_df_interactions.delete_parallel_directly_follows_derived('CASE_AO', 'Application')
        infer_df_interactions.delete_parallel_directly_follows_derived('CASE_AO', 'Offer')
        infer_df_interactions.delete_parallel_directly_follows_derived('CASE_AW', 'Application')
        infer_df_interactions.delete_parallel_directly_follows_derived('CASE_AW', 'Workflow')
        infer_df_interactions.delete_parallel_directly_follows_derived('CASE_WO', 'Workflow')
        infer_df_interactions.delete_parallel_directly_follows_derived('CASE_WO', 'Offer')

def discover_model(db_connection, config):
    semantic_header = SemanticHeader.create_semantic_header(config)
    print(Fore.RED + 'Discovering multi-object DFG.' + Fore.RESET)
    dfg = DiscoverDFG(db_connection=db_connection, semantic_header=semantic_header)
    
    if semantic_header.name == "BPIC15":
        dfg.discover_dfg_for_entity("Application", 25000, 0.0)
        dfg.discover_dfg_for_entity("Resource", 25000, 0.0)
        dfg.discover_dfg_for_entity("MonitoringResource", 25000, 0.0)
        dfg.discover_dfg_for_entity("ResponsibleActor", 25000, 0.0)
    else:
        dfg.discover_dfg_for_entity("Application", 25000, 0.0)
        dfg.discover_dfg_for_entity("Offer", 25000, 0.0)
        dfg.discover_dfg_for_entity("Workflow", 25000, 0.0)
        dfg.discover_dfg_for_entity("CASE_AO", 25000, 0.0)
        dfg.discover_dfg_for_entity("CASE_AW", 25000, 0.0)
        dfg.discover_dfg_for_entity("CASE_WO", 25000, 0.0)


def build_tasks(db_connection, config):
    semantic_header = SemanticHeader.create_semantic_header(config)
    print(Fore.RED + 'Detecting tasks.' + Fore.RESET)
    if semantic_header.name == "BPIC15":
        task_identifier = TaskIdentification(
            db_connection=db_connection,
            semantic_header=semantic_header,
            resource="Resource",
            case="Application")
    else:
        task_identifier = TaskIdentification(
        db_connection=db_connection,
        semantic_header=semantic_header,
        resource="Resource",
        case="CaseAWO")
    task_identifier.identify_tasks()
 
    # task_identifier.aggregate_on_task_variant()


def add_actor_behavior(db_connection, config, analysis_config):
    semantic_header = SemanticHeader.create_semantic_header(config)
    print(Fore.RED + 'Adding actor behavior.' + Fore.RESET)

    if semantic_header.name == "BPIC15":

        decomposition_actor_behavior = DecompositionActorBehavior(db_connection=db_connection,
                                                                semantic_header=semantic_header,
                                                                dataset_name=analysis_config.dataset_name,
                                                                resource="Resource", case="Application")
    else:
        decomposition_actor_behavior = DecompositionActorBehavior(db_connection=db_connection,
                                                              semantic_header=semantic_header,
                                                              dataset_name=analysis_config.dataset_name,
                                                              resource="Resource", case="CaseAWO")
    decomposition_actor_behavior.add_actor_behavior()


def extract_decomposed_performance(db_connection, config, analysis_config):
    semantic_header = SemanticHeader.create_semantic_header(config)
    print(Fore.RED + 'Decomposing performance by actor behavior.' + Fore.RESET)
    
    if semantic_header.name == "BPIC15":

        decomposition_actor_behavior = DecompositionActorBehavior(db_connection=db_connection,
                                                              semantic_header=semantic_header,
                                                              dataset_name=analysis_config.dataset_name,
                                                              resource="Resource", case="Application")
    else:
        decomposition_actor_behavior = DecompositionActorBehavior(db_connection=db_connection,
                                                              semantic_header=semantic_header,
                                                              dataset_name=analysis_config.dataset_name,
                                                              resource="Resource", case="CaseAWO")
    
    decomposition_actor_behavior.extract_decomposed_performance_by_actor_behavior_per_edge(
        case_edges=analysis_config.case_edges, edge_min_freq=analysis_config.edge_min_freq)


def infer_delays(db_connection):
    print(Fore.RED + 'Computing delay edges.' + Fore.RESET)
    delays = PerformanceAnalyzeDelays(db_connection)
    # delays.enrich_with_delay_edges()
    # delays.analyze_delays()
    delays.visualize_delays(10000)


def print_statistics(db_connection):
    db_manager = DBManagement(db_connection)
    db_manager.print_statistics()
