from promg import Query


class DecompositionActorBehaviorQueryLibrary:

    @staticmethod
    def q_add_actor_behavior_continuation(case, resource):
        query_str = '''
            MATCH (e1:Event)-[df:$df_case]->(e2:Event) WHERE (e1)-[:$df_resource]->(e2) 
            SET df.actor_behavior = "continuation"
            '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "df_case": case.get_df_label(),
                         "df_resource": resource.get_df_label()
                     })

    @staticmethod
    def q_add_actor_behavior_interruption(case, resource):
        query_str = '''
            MATCH (e1:Event)-[df:$df_case]->(e2:Event) 
                WHERE (e1)-[:CORR]->(:$resource_node_label)<-[:CORR]-(e2) AND NOT (e1)-[:$df_resource]->(e2) 
            SET df.actor_behavior = "interruption"
            '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "resource_node_label": resource.type,
                         "df_case": case.get_df_label(),
                         "df_resource": resource.get_df_label()
                     })

    @staticmethod
    def q_add_actor_behavior_handover_idle(case, resource):
        query_str = '''
            CALL {
                MATCH (tic:TaskInstance)-[:CONTAINS]->(e1:Event)-[df:$df_case]->(e2:Event)<-[:CONTAINS]-
                    (ti:TaskInstance)<-[:$df_ti_resource]-(tir:TaskInstance)
                WHERE NOT (e1)-[:CORR]->(:$resource_node_label)<-[:CORR]-(e2) AND tir.end_time < tic.end_time
                RETURN df
            UNION
                MATCH (e1:Event)-[df:$df_case]->(e2:Event) WHERE NOT ()-[:$df_resource]->(e2)
                RETURN df
            }
            SET df.actor_behavior = "handover_idle"
            '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "resource_node_label": resource.type,
                         "df_case": case.get_df_label(),
                         "df_resource": resource.get_df_label(),
                         "df_ti_resource": resource.get_df_ti_label()
                     })

    @staticmethod
    def q_add_actor_behavior_handover_prioritized(case, resource):
        query_str = '''
            MATCH (tic:TaskInstance)-[:CONTAINS]->(e1:Event)-[df:$df_case]->(e2:Event)<-[:CONTAINS]-
                (ti:TaskInstance)<-[:$df_ti_resource]-(tir:TaskInstance) 
            WHERE NOT (e1)-[:CORR]->(:$resource_node_label)<-[:CORR]-(e2) AND tir.start_time < tic.end_time < tir.end_time
            SET df.actor_behavior = "handover_prioritized"
            '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "resource_node_label": resource.type,
                         "df_case": case.get_df_label(),
                         "df_ti_resource": resource.get_df_ti_label()
                     })

    @staticmethod
    def q_add_actor_behavior_handover_deprioritized(case, resource):
        query_str = '''
            MATCH (tic:TaskInstance)-[:CONTAINS]->(e1:Event)-[df:$df_case]->(e2:Event)<-[:CONTAINS]-
                (ti:TaskInstance)<-[:$df_ti_resource]-(tir:TaskInstance) 
            WHERE NOT (e1)-[:CORR]->(:$resource_node_label)<-[:CORR]-(e2) AND tic.end_time < tir.start_time
            SET df.actor_behavior = "handover_deprioritized"
            '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "resource_node_label": resource.type,
                         "df_case": case.get_df_label(),
                         "df_ti_resource": resource.get_df_ti_label()
                     })

    @staticmethod
    def q_get_all_df_edges_activity_lifecycle(case, min_freq):
        query_str = '''
                MATCH (e1:Event)-[df:$df_case]->(e2:Event)
                WITH DISTINCT e1.activity AS activity1, e1.lifecycle AS lifecycle1, e2.activity AS activity2, 
                    e2.lifecycle AS lifecycle2, count(*) AS count where count > $min_freq
                RETURN activity1, lifecycle1, activity2, lifecycle2, count order by count desc
                '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "df_case": case.get_df_label(),
                         "min_freq": min_freq
                     })

    @staticmethod
    def q_get_all_actor_behavior_per_df(case, resource, edge_tuple):
        query_str = '''
            MATCH (e1:Event {activity: "$activity1", lifecycle:"$lifecycle1"})-[df:$df_case]->
                (e2:Event {activity: "$activity2", lifecycle:"$lifecycle2"})
            WITH e1.timestamp AS time, duration.inSeconds(e1.timestamp, e2.timestamp) AS duration, 
                df.actor_behavior AS actor_behavior
            RETURN time, duration, actor_behavior
            '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "resource_node_label": resource.type,
                         "df_case": case.get_df_label(),
                         "activity1": edge_tuple[0][0],
                         "lifecycle1": edge_tuple[0][1],
                         "activity2": edge_tuple[1][0],
                         "lifecycle2": edge_tuple[1][1]
                     })

    @staticmethod
    def q_get_continuation_per_df(case, resource, edge_tuple):
        query_str = '''
            MATCH (e1:Event {activity: "$activity1", lifecycle:"$lifecycle1"})-[df:$df_case]->
                (e2:Event {activity: "$activity2", lifecycle:"$lifecycle2"})<-[:CONTAINS]-
                (ti:TaskInstance)-[:CORR]->(n:$resource_node_label)
            WHERE df.actor_context IN ["continuation"]
            WITH e1.timestamp AS time, duration.inSeconds(e1.timestamp, e2.timestamp) AS duration, 
                df.actor_context AS actor_context, ti.cluster AS task, n.sysId AS actor
            RETURN time, duration, actor_context, task, actor
            '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "resource_node_label": resource.type,
                         "df_case": case.get_df_label(),
                         "activity1": edge_tuple[0][0],
                         "lifecycle1": edge_tuple[0][1],
                         "activity2": edge_tuple[1][0],
                         "lifecycle2": edge_tuple[1][1]
                     })

    @staticmethod
    def q_get_interruption_per_df(case, resource, edge_tuple):
        query_str = '''
            MATCH (e1:Event {activity: "$activity1", lifecycle:"$lifecycle1"})-[df:$df_case]->
                (e2:Event {activity: "$activity2", lifecycle:"$lifecycle2"})<-[:CONTAINS]-
                (ti:TaskInstance)-[:CORR]->(n:$resource_node_label)
            WHERE df.actor_context IN ["interruption"]
            WITH e1.timestamp AS time, duration.inSeconds(e1.timestamp, e2.timestamp) AS duration, 
                df.actor_context AS actor_context, ti.cluster AS task, n.sysId AS actor
            RETURN time, duration, actor_context, task, actor
            '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "resource_node_label": resource.type,
                         "df_case": case.get_df_label(),
                         "activity1": edge_tuple[0][0],
                         "lifecycle1": edge_tuple[0][1],
                         "activity2": edge_tuple[1][0],
                         "lifecycle2": edge_tuple[1][1]
                     })
