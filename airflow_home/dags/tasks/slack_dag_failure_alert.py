from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from pytz import timezone

SLACK_CONN_ID = 'slack'


def replace_airflow_url_prefix(original_error_url, original_url_prefix,
                               replacement_url_prefix='http://ec2-34-220-229-223.us-west-2.compute.amazonaws.com'):
    return original_error_url.replace(original_url_prefix, replacement_url_prefix)


def slack_failed_task(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :ballmer: Task Failed.  
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date').astimezone(timezone('US/Pacific')),
        log_url=replace_airflow_url_prefix(context.get('task_instance').log_url, 'http://localhost'),
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)
