from datetime import datetime, timezone
import json
from airflow.models.baseoperator import BaseOperator



class ProcessYahooAPIOperator(BaseOperator):

    def __init__(self, columns_of_interest=None, **kwargs):
        super().__init__(**kwargs)
        self.columns_of_interest = columns_of_interest

    def execute(self, context):
        task_instance = context['task_instance']
        data = json.loads(task_instance.xcom_pull(task_ids='query_data'))
        data = data['quoteResponse']['result']
        if self.columns_of_interest is None:
            return data
        assert type(self.columns_of_interest) is dict
        rows = []
        for i in range(len(data)):
            row = dict()
            for key, val in self.columns_of_interest.items():
                if val not in data[i]:
                    row[key] = None
                else:
                    row[key] = data[i][val]
            if 'regularMarketTime' not in data[i]:
                regular_marketTime = None
            else:
                regular_marketTime = data[i]['regularMarketTime']
            row['lastTradeUTC']  = regular_marketTime
            rows.append(row)
        return rows