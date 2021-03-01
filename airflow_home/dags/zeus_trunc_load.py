from tasks.database_conn import db_conn
from datetime import datetime
from datetime import timedelta
from dag_objects import RedshiftCopy, TableColumns
from airflow.models import DAG, Variable
from tasks.slack_dag_failure_alert import slack_failed_task
from tasks.slack_sla_missed_alert import slack_sla_missed_task
from airflow.operators.postgres_operator import PostgresOperator
from airflow_home.plugins.postgres_table_defs_to_s3 import PostgresTableDefinitionsToS3
from airflow_home.plugins.postgres_data_to_s3 import PostgresToS3
from airflow_home.plugins.create_postgres_table_from_s3 import CreatePostgresTableFromS3Definitions

default_args = {
    'on_failure_callback': slack_failed_task
    , 'owner': 'pslack'
    , 'priority_weight': 5
    , 'retries': 1
    , 'sla': timedelta(hours=2)
}

dag = DAG(
    dag_id='zeus_trunc_load'
    , description='Rebuilds Zeus tables in nextstepdb nightly'
    , schedule_interval='10 7 * * *'
    , start_date=datetime(2020, 4, 26)
    , catchup=False
    , default_args=default_args
    , tags=['core']
)


def generate_truncate_tasks(t_name):
    truncate_existing_table = PostgresOperator(
        task_id='truncate_%s_table' % t_name
        , sql=("""TRUNCATE zeus.%s""" % t_name)
        , postgres_conn_id='redshift'
        , database='nextstepdb'
        , sla_miss_callback=slack_sla_missed_task
        , dag=dag
    )

    return truncate_existing_table


def generate_drop_table_tasks(t_name):
    drop_redshift_table = PostgresOperator(
        task_id='drop_%s_redshift_table' % t_name
        , sql="drop table if exists zeus.%s" % t_name
        , postgres_conn_id='redshift'
        , database='nextstepdb'
        , sla_miss_callback=slack_sla_missed_task
        , dag=dag
    )

    rebuild_redshift_table = CreatePostgresTableFromS3Definitions(
        task_id='rebuild_%s_redshift_table' % t_name
        , bucket='nextstepetlprod'
        , source_table='%s' % t_name
        , table_def_s3_key='zeus_%s_table_definitions.csv' % t_name
        , postgres_conn_id='redshift'
        , schema='zeus'
        , sla_miss_callback=slack_sla_missed_task
        , dag=dag
    )

    return drop_redshift_table, rebuild_redshift_table


table_list = [
    'answer_choices'
    , 'assessment_block_progresses'
    , 'assessment_block_questions'
    , 'assessment_blocks'
    , 'calendar_events'
    , 'calendar_invitees'
    , 'certificate_templates'
    , 'certificates'
    , 'course_progresses'
    , 'course_skillsets'
    , 'courses'
    , 'demo_video_progresses'
    , 'demo_videos'
    , 'employers'
    , 'events'
    , 'intercom_skill_events'
    , 'intro_progresses'
    , 'intros'
    , 'learners'
    , 'legacy_skills'
    , 'legacy_skillsets'
    , 'question_answer_choices'
    , 'questions'
    , 'reminders'
    , 'skill_intros'
    , 'skill_progresses'
    , 'skill_steps'
    , 'skill_video_challenges'
    , 'skills'
    , 'skillset_progresses'
    , 'skillset_skills'
    , 'skillsets'
    , 'state_configs'
    , 'step_assessment_blocks'
    , 'step_demo_videos'
    , 'step_progresses'
    , 'step_success_criteria'
    , 'steps'
    , 'study_plans'
    , 'success_criteria_progresses'
    , 'video_challenge_progresses'
    , 'videos'
]

# Configure to determine how many parallel tasks to run from Airflow UI. Do not bother with more than 16.
parallel_runs = int(Variable.get("zeus_parallel_runs", 4)) - 1
last_task_list = []
last_task = None
for num, zeus_table_name in enumerate(table_list):

    def compare_tables():

        """
        Compares columns from the source table to see if they match the
        existing destination table.
        If source table != destination table, trigger a rebuild of the table.
        """

        conn, cursor = db_conn('redshift')
        cursor.execute(
            TableColumns(
                table='%s' % zeus_table_name
                , schema='zeus').redshift_sql())
        redshift_df = map(lambda x: x[0], cursor.fetchall())
        conn.close()

        conn, cursor = db_conn('zeus')
        cursor.execute(
            TableColumns(
                table='%s' % zeus_table_name
                , schema='public').zeus_sql())
        zeus_df = map(lambda x: x[0], cursor.fetchall())
        conn.close()

        if sorted(zeus_df) != sorted(redshift_df):
            return 'drop_%s_redshift_table' % zeus_table_name
        else:
            return 'truncate_%s_table' % zeus_table_name


    table_def_to_s3 = PostgresTableDefinitionsToS3(
        task_id='%s_table_def_to_s3' % zeus_table_name
        , bucket='nextstepetlprod'
        , s3_file_key='zeus_%s_table_definitions.csv' % zeus_table_name
        , table_name='%s' % zeus_table_name
        , postgres_conn_id='zeus'
        , sla_miss_callback=slack_sla_missed_task
        , dag=dag
    )

    load_table_data_to_s3 = PostgresToS3(
        task_id='load_zeus_%s_table_data_to_s3' % zeus_table_name
        , table_name='%s' % zeus_table_name
        , bucket='nextstepetlprod'
        , table_def_s3_key='zeus_%s_table_definitions.csv' % zeus_table_name
        , postgres_conn_id='zeus'
        , sla_miss_callback=slack_sla_missed_task
        , dag=dag
    )

    load_data_from_s3 = PostgresOperator(
        task_id='copy_%s_data_from_s3' % zeus_table_name
        , sql=RedshiftCopy(
            destination_table_name='zeus.%s' % zeus_table_name
            , s3_file='zeus_%s_table.csv' % zeus_table_name
            , maxerror=100
        ).sql()
        , postgres_conn_id='redshift'
        , database='nextstepdb'
        , sla_miss_callback=slack_sla_missed_task
        , dag=dag
    )

    if last_task and compare_tables() == 'truncate_%s_table' % zeus_table_name:

        truncate_existing_table = generate_truncate_tasks(zeus_table_name)

        last_task >> table_def_to_s3
        table_def_to_s3 >> load_table_data_to_s3
        load_table_data_to_s3 >> truncate_existing_table
        truncate_existing_table >> load_data_from_s3

    elif (last_task and
          compare_tables() == 'drop_%s_redshift_table' % zeus_table_name):

        drop_redshift_table, rebuild_redshift_table = (
            generate_drop_table_tasks(zeus_table_name))

        last_task >> table_def_to_s3
        table_def_to_s3 >> load_table_data_to_s3
        load_table_data_to_s3 >> drop_redshift_table
        drop_redshift_table >> rebuild_redshift_table
        rebuild_redshift_table >> load_data_from_s3

    elif last_task is None and compare_tables() == (
            'truncate_%s_table' % zeus_table_name):

        truncate_existing_table = generate_truncate_tasks(zeus_table_name)

        table_def_to_s3 >> load_table_data_to_s3
        load_table_data_to_s3 >> truncate_existing_table
        truncate_existing_table >> load_data_from_s3

    elif last_task is None and compare_tables() == (
            'drop_%s_redshift_table' % zeus_table_name):

        drop_redshift_table, rebuild_redshift_table = (
            generate_drop_table_tasks(zeus_table_name))

        table_def_to_s3 >> load_table_data_to_s3
        load_table_data_to_s3 >> drop_redshift_table
        drop_redshift_table >> rebuild_redshift_table
        rebuild_redshift_table >> load_data_from_s3

    last_task_list.append(load_data_from_s3)

    if num < parallel_runs:
        last_task = None
    else:
        last_task = last_task_list[(num - parallel_runs)]

