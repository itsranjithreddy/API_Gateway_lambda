import pymysql
from flask import json
import os
from jinja2 import Template
import ast
from decimal import Decimal

# TODO: Convert this lambda function to use SAM for dev/test/deployment
# adding these feilds in environmental variables
endpoint=os.environ['endpoint']
username = os.environ['username']
password = os.environ['password']
database_name = os.environ['database_name']


#Connection
conn = pymysql.connect(host=endpoint,user=username, passwd=password, db=database_name, cursorclass=pymysql.cursors.DictCursor)
cursor = conn.cursor()

class CustomJsonEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(CustomJsonEncoder, self).default(obj)

def lambda_handler(event, context):
    # getting in to db according to the tenent id input
    qs_params = event.get('queryStringParameters')
    tenant_id = qs_params.get('tenant_id')
    path = event.get('path')
    tenant_db = "select * from master where tenant_id=%s"
    cursor.execute(tenant_db, (tenant_id,))
    tenant_details = cursor.fetchone()
    tenant_db_name = tenant_details['db_name']
    tenant_name = tenant_details['bot_name']
    print(f'path: {path}')
    
    if path == "/report/bot/visitor-count":   
        query=f"select date(session_start) as visited_date, count(id) as avg_visitors from {tenant_db_name}.session group by date(session_start) order by visited_date DESC"
        template_file = open('./template/avg_visitors.html', 'r').read()
        date_val='visited_date'
    
    elif path == "/report/bot/conversation-volume":
        query=f"SELECT COUNT(request_text) AS volume, date(request_time) AS per_day from {tenant_db_name}.chat_conversation GROUP BY date(request_time) order by per_day DESC"
        template_file = open('./template/volume.html', 'r').read()
        date_val='per_day'
        
    elif path == "/report/bot/miss-conversations":    
        query=f"SELECT request_text, count(hit_miss) as miss_hit FROM {tenant_db_name}.chat_conversation where hit_miss in('miss') GROUP by request_text order by miss_hit DESC"
        template_file = open('./template/miss_conversation.html', 'r').read()
        date_val=0
        
    
    elif path == "/report/bot/performance":
        query=f"SELECT Date, (hit_count*100/num_conversations) as Bot_Performance from (SELECT date(request_time) AS Date, Count(1) as num_conversations, SUM(Case when hit_miss = 'hit' then 1 else 0 end ) AS hit_count, SUM(Case when hit_miss = 'miss' then 1 else 0 end ) AS mis_count from {tenant_db_name}.chat_conversation GROUP BY date(request_time)) as a order by Date DESC"
        template_file = open('./template/performance.html','r').read()
        date_val='Date'
    
    elif path == "/report/bot/visitor-duration":
        query=f"SELECT id AS Visitor, DATE(session_start) AS Visited_date, TIME_TO_SEC(TIMEDIFF(session_last_update, session_start))/60 AS Visitor_duration_in_min from {tenant_db_name}.session order by Visited_date DESC"
        template_file = open('./template/visitor_duration.html','r').read()
        date_val='Visited_date'
    
    elif path == "/report/bot/intents":
        query=f"SELECT request_text , count(request_text) as number_of_questions FROM {tenant_db_name}.chat_conversation where hit_miss in('hit') GROUP by request_text order by count(request_text) DESC;"
        template_file= open('./template/intent.html','r').read()
        date_val=0
    
    else:
        pass
    
    cursor.execute(query)   
    res = cursor.fetchall()
    tenant_data = json.dumps(res, cls=CustomJsonEncoder)
    tenant_data = ast.literal_eval(tenant_data)

    for tenant in tenant_data:
        if date_val !=0:
            tenant[date_val]=tenant[date_val][0:11]


    data = {
        'tenant_data': tenant_data,
        'tenant_id': tenant_id,
        'tenant_name': tenant_name
        
    }                           

    template = Template(template_file)
    response = {
            "statusCode": 200,            
            "headers": {
                'Content-Type': 'text/html'
            },
            'body': template.render(data)
        }

    return response              
