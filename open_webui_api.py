import requests

def upload(path):
    headers = {
        'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjFhYjBmMjJiLTU2NjEtNGE3Yy05NTgzLThkODY3ZmVlM2ZkMSJ9.CvG0q4mJ8MST2Iv73kO0IvKKbge0FEab2ei3zg6Mibw',
    }

    files = {
        'file': open(path, 'rb'),
    }

    response = requests.post('http://localhost:3000/api/v1/files/', headers=headers, files=files)
    response = response.json()
    id = response['id']
    return id

def ask(query, file_id):
    headers = {
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjFhYjBmMjJiLTU2NjEtNGE3Yy05NTgzLThkODY3ZmVlM2ZkMSJ9.CvG0q4mJ8MST2Iv73kO0IvKKbge0FEab2ei3zg6Mibw',
        'Content-Type': 'application/json',
    }

    json_data = {
        'model': 'gpt-4-turbo',
        'messages': [
            {
                'role': 'user',
                'content': query,
            },
        ],
        'files': [
            {
                'type': 'file',
                'id': f'{file_id}',
            },
        ],
    }
    print(json_data)
    response = requests.post('http://localhost:3000/api/chat/completions', headers=headers, json=json_data)
    response = response.json()
    response = response['choices'][0]['message']['content']
    return response

try:
    id = upload('laws.sql')
    print(id)
    query = 'which tables are being created in the sql file i provide you with"'
    response = ask(query, id)
    print(response)
except Exception as err:
    print('ERROR', err)

