import os
import pandas as pd
from os import path
import re

from promg import DatabaseConnection
from promg.data_managers.semantic_header import ConstructedNodes, SemanticHeader

from queries.decomposition_actor_behavior import DecompositionActorBehaviorQueryLibrary as ql
from queries import query_result_parser as qp


class DecompositionActorBehavior:

    def __init__(self, db_connection, semantic_header, dataset_name, resource: str, case: str):
        self.connection = db_connection
        self.dataset_name = dataset_name
        self.resource: ConstructedNodes = semantic_header.get_entity(resource)
        self.case: ConstructedNodes = semantic_header.get_entity(case)

        self.intermediate_output_directory = f"output_intermediate\\{dataset_name}\\decomposed_actor_behavior\\"
        os.makedirs(self.intermediate_output_directory, exist_ok=True)
        self.output_directory = f"output_final\\{dataset_name}\\decomposed_actor_behavior\\"
        os.makedirs(self.output_directory, exist_ok=True)

    def add_actor_behavior(self):
        kwargs = {"case": self.case, "resource": self.resource}
        self.connection.exec_query(ql.q_add_actor_behavior_continuation, **kwargs)
        self.connection.exec_query(ql.q_add_actor_behavior_interruption, **kwargs)
        self.connection.exec_query(ql.q_add_actor_behavior_handover_idle, **kwargs)
        self.connection.exec_query(ql.q_add_actor_behavior_handover_prioritized, **kwargs)
        self.connection.exec_query(ql.q_add_actor_behavior_handover_deprioritized, **kwargs)

    def extract_decomposed_performance_by_actor_behavior_per_edge(self, case_edges, edge_min_freq: int = 1000,
                                                                  time_unit: str = 'hours',
                                                                  agg_func=None,
                                                                  exclude_zero_duration: bool = True):
        if agg_func is None:
            agg_func = ["mean", "median", "min", "max", "std", "count"]
        if case_edges == 'all' and self.dataset_name == "BPIC17":
            self.case_edges = qp.parse_to_2d2tuple_list(
                self.connection.exec_query(ql.q_get_all_df_edges_activity_lifecycle,
                                           **{"case": self.case, "min_freq": edge_min_freq}),
                "activity1", "lifecycle1", "activity2", "lifecycle2")
            
        elif case_edges == 'all' and self.dataset_name == "BPIC15":
             self.case_edges = qp.parse_to_2d_list(
                self.connection.exec_query(
                    ql.q_get_all_df_edges_activity, **{"case": self.case, "min_freq": edge_min_freq}
                ),
                "activity1", "activity2"
            )
        else:
            self.case_edges = case_edges
        
        if self.dataset_name == "BPIC15":
            list_df_instances_by_actor_behavior = get_df_instances_by_actor_behavior_bpic15(
                intermediate_output_directory=self.intermediate_output_directory, connection=self.connection,
                case=self.case, resource=self.resource, time_unit=time_unit, case_edges=self.case_edges)
        else:
            list_df_instances_by_actor_behavior = get_df_instances_by_actor_behavior_bpic17(
                intermediate_output_directory=self.intermediate_output_directory, connection=self.connection,
                case=self.case, resource=self.resource, time_unit=time_unit, case_edges=self.case_edges)
            
        groupby = ["actor_behavior"]
        groupby_str = ", ".join(groupby)
        actor_behavior_agg_all = []

        if self.dataset_name == "BPIC17":
            for i, df_instances_per_edge in enumerate(list_df_instances_by_actor_behavior):
                df_actor_behavior_agg = aggregate_actor_behavior(df_instances_per_edge,
                                                                 groupby,
                                                                 agg_func,
                                                                 time_unit,
                                                                 exclude_zero_duration=exclude_zero_duration)
                df_actor_behavior_agg = pd.concat([df_actor_behavior_agg],
                                                keys=[f"{self.case_edges[i][1][0]}-{self.case_edges[i][1][1]}"],
                                                names=["sink"])
                df_actor_behavior_agg = pd.concat([df_actor_behavior_agg],
                                                keys=[f"{self.case_edges[i][0][0]}-{self.case_edges[i][0][1]}"],
                                                names=["source"])
                actor_behavior_agg_all.append(df_actor_behavior_agg)
            df_concat_actor_behavior_agg = pd.concat(actor_behavior_agg_all, axis=0, ignore_index=False)
            df_concat_actor_behavior_agg.to_csv(
                f"{self.output_directory}\\performance_decomposed_by_{groupby_str}.csv")
        else:
            for i, df_instances_per_edge in enumerate(list_df_instances_by_actor_behavior):
                df_actor_behavior_agg = aggregate_actor_behavior(df_instances_per_edge,
                                                                 groupby,
                                                                 agg_func,
                                                                 time_unit,
                                                                 exclude_zero_duration=exclude_zero_duration)

                source_activity, target_activity = self.case_edges[i]
                df_actor_behavior_agg = pd.concat([df_actor_behavior_agg],
                                                keys=[target_activity],
                                                names=["sink"])
                df_actor_behavior_agg = pd.concat([df_actor_behavior_agg],
                                                keys=[source_activity],
                                                names=["source"])

                actor_behavior_agg_all.append(df_actor_behavior_agg)

            df_concat_actor_behavior_agg = pd.concat(actor_behavior_agg_all, axis=0, ignore_index=False)
            df_concat_actor_behavior_agg.to_csv(
                f"{self.output_directory}\\performance_decomposed_by_{groupby_str}.csv"
            )


def aggregate_actor_behavior(df_to_aggregate, groupby, agg_func, time_unit, exclude_zero_duration=True):
    duration_col = f"duration_{time_unit}"
    df_filtered = df_to_aggregate.copy()

    if isinstance(agg_func, str) or not hasattr(agg_func, "__iter__"):
        agg_funcs = [agg_func] if agg_func is not None else []
    else:
        agg_funcs = list(agg_func)

    if exclude_zero_duration and duration_col in df_filtered.columns:
        df_filtered = df_filtered[df_filtered[duration_col].fillna(0) > 0]

    if df_filtered.empty:
        column_tuples = [("actor_behavior", "count"), ("actor_behavior", "percentage")]
        column_tuples.extend((duration_col, str(func)) for func in agg_funcs)
        columns = pd.MultiIndex.from_tuples(column_tuples)
        return pd.DataFrame(columns=columns)

    total_count = len(df_filtered) / 2
    total_count = total_count if total_count else 1

    counts = df_filtered.groupby(groupby)["actor_behavior"].count()
    percentage = counts / total_count if total_count else counts * 0

    duration_agg = (
        df_filtered.groupby(groupby)[duration_col].agg(agg_funcs) if agg_funcs else pd.DataFrame()
    )
    if isinstance(duration_agg, pd.Series):
        duration_agg = duration_agg.to_frame()

    counts_df = counts.to_frame()
    counts_df.columns = pd.MultiIndex.from_tuples([("actor_behavior", "count")])

    percentage_df = percentage.to_frame()
    percentage_df.columns = pd.MultiIndex.from_tuples([("actor_behavior", "percentage")])

    if not duration_agg.empty:
        duration_agg.columns = pd.MultiIndex.from_tuples(
            [(duration_col, str(col)) for col in duration_agg.columns]
        )

    frames = [counts_df, percentage_df]
    if not duration_agg.empty:
        frames.append(duration_agg)

    result = pd.concat(frames, axis=1)
    return result


# def aggregate_actor_behavior(df_to_aggregate, groupby, agg_func, time_unit):
#     if list(set(df_to_aggregate['actor_behavior'].unique().tolist()) & {"interruption", "handover_idle",
#                                                                        "handover_prioritized",
#                                                                        "handover_deprioritized"}):
#         df_aggregated = df_to_aggregate.groupby(groupby).agg(
#             {"actor_behavior": ["count", ("percentage", lambda x: x.count() / len(df_to_aggregate) * 2)],
#              f"duration_{time_unit}": agg_func, "nr_prioritized": agg_func, "nr_idle": agg_func})
#     else:
#         df_aggregated = df_to_aggregate.groupby(groupby).agg(
#             {"actor_behavior": ["count", ("percentage", lambda x: x.count() / len(df_to_aggregate) * 2)],
#              f"duration_{time_unit}": agg_func})
#     df_aggregated[('actor_behavior', 'percentage')] = df_aggregated[('actor_behavior', 'count')] / df_aggregated[
#         ('actor_behavior', 'count')].sum()
#     return df_aggregated


# def convert_2d_string_list(str_list, delimiter1=',', delimiter2='-'):
#     converted_list = str_list.split(delimiter1)
#     for index in range(0, len(converted_list)):
#         converted_list[index] = converted_list[index].split(delimiter2)
#     return converted_list
#
#
# def extract_stringlist_to_columns(df, column_name):
#     for index, row in df.iterrows():
#         if pd.isna(row[f"{column_name}"]):
#             continue
#         else:
#             prioritized_task_count_list = convert_2d_string_list(row[f"{column_name}"])
#             for prioritized_task in prioritized_task_count_list:
#                 df.loc[index, f"{column_name}_{prioritized_task[0]}"] = prioritized_task[1]
#     return df

def get_df_instances_by_actor_behavior_bpic15(intermediate_output_directory, connection, case, resource, time_unit, case_edges):
    list_df_instances_by_actor_behavior = []

    for case_edge in case_edges:
        try:
            str_case_edge = f"{case_edge[0]}_{case_edge[1]}"
        except IndexError:
            print("Invalid edge tuple structure:", case_edge)

        # Sanitize filename
        safe_edge_name = re.sub(r'[\\/:*?"<>|]', '_', str_case_edge)
        filepath = f"{intermediate_output_directory}actor_behavior_{safe_edge_name}.pkl"

        # Load or compute per-edge dataframe
        if path.exists(filepath):
            df_instances_by_actor_behavior = pd.read_pickle(filepath)
        else:
            df_instances_by_actor_behavior = get_instances_by_actor_behavior_per_df_bpic15(
                connection, case, resource, time_unit, case_edge
            )
            df_instances_by_actor_behavior.to_pickle(filepath)

        list_df_instances_by_actor_behavior.append(df_instances_by_actor_behavior)

    return list_df_instances_by_actor_behavior


def get_df_instances_by_actor_behavior_bpic17(intermediate_output_directory, connection, case, resource, time_unit,
                                       case_edges):
    list_df_instances_by_actor_behavior = []
    for case_edge in case_edges:
        try:
            str_case_edge = f"{case_edge[0][0]}-{case_edge[0][1]}_{case_edge[1][0]}-{case_edge[1][1]}"
        except IndexError:
            print("Your tuple does not have that index")

        if path.exists(
                f"{intermediate_output_directory}actor_behavior_{str_case_edge}.pkl"):
            df_instances_by_actor_behavior = pd.read_pickle(
                f"{intermediate_output_directory}actor_behavior_{str_case_edge}.pkl")
        else:
            df_instances_by_actor_behavior = get_instances_by_actor_behavior_per_df_bpic17(connection, case, resource,
                                                                                    time_unit, case_edge)
            df_instances_by_actor_behavior.to_pickle(
                f"{intermediate_output_directory}actor_behavior_{str_case_edge}.pkl")
        # if "prioritized_tasks" in df_actor_behavior.columns:
        #     df_actor_behavior = extract_stringlist_to_columns(df_actor_behavior, "prioritized_tasks")
        # for column in ([value for value in df_actor_behavior.columns if
        #                 value in ['nr_prioritized', 'nr_idle', 'backlog_start', 'backlog_end']] +
        #                [col for col in df_actor_behavior if col.startswith('prioritized_tasks_')]):
        #     df_actor_behavior[column] = pd.to_numeric(df_actor_behavior[column].fillna(0))
        # for column in ([value for value in df_instances_by_actor_behavior.columns if
        #                 value in ['nr_prioritized', 'nr_idle']]):
        #     df_instances_by_actor_behavior[column] = pd.to_numeric(df_instances_by_actor_behavior[column].fillna(0))
        list_df_instances_by_actor_behavior.append(df_instances_by_actor_behavior)
    return list_df_instances_by_actor_behavior

def get_instances_by_actor_behavior_per_df_bpic15(connection, case, resource, time_unit, case_edge):
    # Execute the actor behavior query for a given (activity1, activity2)
    df_instances_by_actor_behavior = qp.parse_to_dataframe(
        connection.exec_query(
            ql.q_get_all_actor_behavior_per_df,
            **{"case": case, "resource": resource, "edge_tuple": case_edge},
        ),
        timedelta_cols={"duration": time_unit},
        timestamp_cols=["startTime", "completeTime"],
    )

    # Add an 'all' category for comparison
    df_instances_all = df_instances_by_actor_behavior.copy()
    df_instances_all["actor_behavior"] = "all"

    df_instances_per_df = pd.concat(
        [df_instances_by_actor_behavior, df_instances_all], ignore_index=True, sort=False
    )
    return df_instances_per_df

def get_instances_by_actor_behavior_per_df_bpic17(connection, case, resource, time_unit, case_edge):
    df_instances_by_actor_behavior = qp.parse_to_dataframe(
        connection.exec_query(ql.q_get_all_actor_behavior_per_df_bpic17,
                              **{"case": case, "resource": resource, "edge_tuple": case_edge}),
        timedelta_cols={"duration": time_unit}, timestamp_cols=["time"])
    df_instances_all = df_instances_by_actor_behavior.copy()
    df_instances_all["actor_behavior"] = "all"

    df_instances_per_df = pd.concat([df_instances_by_actor_behavior, df_instances_all], ignore_index=True, sort=False)
    return df_instances_per_df
