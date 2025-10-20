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


    def extract_decomposed_performance_by_actor_behavior_per_edge(
            self, case_edges, edge_min_freq: int = 1000, time_unit: str = 'hours', agg_func=["mean"]
    ):
        # If user wants all edges, retrieve them dynamically (no lifecycle now)
        if case_edges == 'all':
            self.case_edges = qp.parse_to_2d_list(
                self.connection.exec_query(
                    ql.q_get_all_df_edges_activity, **{"case": self.case, "min_freq": edge_min_freq}
                ),
                "activity1", "activity2"
            )
        else:
            self.case_edges = case_edges

        # Fetch dataframes for all edges
        list_df_instances_by_actor_behavior = get_df_instances_by_actor_behavior(
            intermediate_output_directory=self.intermediate_output_directory,
            connection=self.connection,
            case=self.case,
            resource=self.resource,
            time_unit=time_unit,
            case_edges=self.case_edges,
        )

        # Aggregate
        groupby = ["actor_behavior"]
        groupby_str = ", ".join(groupby)
        actor_behavior_agg_all = []

        for i, df_instances_per_edge in enumerate(list_df_instances_by_actor_behavior):
            df_actor_behavior_agg = aggregate_actor_behavior(df_instances_per_edge, groupby, agg_func, time_unit)

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



def aggregate_actor_behavior(df_to_aggregate, groupby, agg_func, time_unit):
    total_count = 0.5 * len(df_to_aggregate)
    df_aggregated = df_to_aggregate.groupby(groupby).agg(
        {
            "actor_behavior": ["count", ("percentage", lambda x: x.count() / total_count)],
            f"duration_{time_unit}": agg_func,
        }
    )
    return df_aggregated


def get_df_instances_by_actor_behavior(intermediate_output_directory, connection, case, resource, time_unit, case_edges):
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
            df_instances_by_actor_behavior = get_instances_by_actor_behavior_per_df(
                connection, case, resource, time_unit, case_edge
            )
            df_instances_by_actor_behavior.to_pickle(filepath)

        list_df_instances_by_actor_behavior.append(df_instances_by_actor_behavior)

    return list_df_instances_by_actor_behavior


def get_instances_by_actor_behavior_per_df(connection, case, resource, time_unit, case_edge):
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
