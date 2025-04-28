import os
import pandas as pd
import json

from psql import Postgre
from openai import OpenAI, AsyncOpenAI

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

system_prompt = '''
You are an expert system for analyzing citizen appeals to government entities. Your task is to extract meaningful tags from each appeal that capture its core themes, requests, and concerns.

Guidelines for tag generation:
1. Create 3-8 relevant tags per appeal depending on its complexity
2. Focus on identifying:
   - The main issue or problem
   - The government service or department involved
   - The type of request (complaint, inquiry, application, etc.)
   - Any specific policies or regulations mentioned
   - The desired outcome or resolution

3. Format each tag as a single word or short phrase (2-3 words maximum)
4. Use standardized terminology where possible
5. DO NOT include in tags:
   - Any sensitive information marked as "HIDDEN" or "*"
   - Personal identifiers or names
   - Specific dates, case numbers, or locations that could identify individuals

6. Ensure tags reflect the meaning and intent of the appeal, not just surface keywords
7. Include tags for both explicit and implicit themes


Appeals will be either in Russian or Kazakh language only!
Output of tags must be in Russian language only!

Output format must be in json:
{{
    "тэги": ["тэг1", "тэг2", и так далее]
}}
'''


user_prompt = '''
Please analyze the following appeal(s) and generate appropriate tags:

EXAMPLE:
Input: "I submitted documents for my residence permit renewal to HIDDEN on January 15, but after two months I still haven't received any response or update on my application status. My current permit expires in 10 days. Please help."

Appeal #1: I submitted documents for my residence permit renewal to HIDDEN...
Tags: residence_permit, application_delay, document_processing, permit_expiration, urgent, immigration, administrative_backlog

Now, analyze the following appeal(s):

Appeals will be either in Russian or Kazakh language only!
Output of tags must be in Russian language only!

{}
'''

df = pd.read_csv("eobr_enrichhed.csv", sep=';')
print(df)

host = os.getenv('NIT_DB_HOST')
user = os.getenv('NIT_DB_USER')
password = os.getenv('NIT_DB_PASSWORD')
port = os.getenv('NIT_DB_PORT')
database = os.getenv('NIT_DB_DATABASE')


psql = Postgre()
psql.connect(host, user, password, port, database)
insert_query = '''
    insert into tags (id, tag)
    values ('{}', '{}')
'''

for index, rows in df.iterrows():
    try:
        if index > 10:
            break
        id = rows['id']
        data = rows['DATA']
        name_tu_type = rows['name_tu_type']
        name_ru_org = rows['name_ru_org']

        inp = {
            'data': data,
            'type': name_tu_type,
            'organization': name_ru_org
        }
        inp = json.dumps(inp)
        print('id', id)
        print('data', data)
        print('name_tu', name_tu_type)
        print('name_ru', name_ru_org)

        response = client.chat.completions.create(
            model="gpt-4o",  # or gpt-3.5-turbo
            messages=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": user_prompt.format(inp)
                }
            ],
            temperature=0.1
        )
        ans = response.choices[0].message.content
        ans = ans.split('{')[1]
        ans = ans.split('}')[0]
        ans = '{\n' + ans + '\n}'
        ans = json.loads(ans)
        print(ans, type(ans))
        for elem in ans['тэги']:
            elem = elem.replace('_', ' ').lower()
            cur_query = insert_query.format(id, elem)
            try:
                psql.execute_query_params(cur_query, query_type='insert')
            except Exception as int_err:
                print('INSERT ERROR', int_err)
            print(elem)
        print()
    except Exception as err:
        print('ERROR', err)
    

psql.disconnect()