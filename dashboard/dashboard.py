from dash import Dash, html, dcc, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from kafka import KafkaConsumer
import json
from collections import OrderedDict

def create_kafka_consumer(topic_name):
    # Set up a Kafka consumer with specified topic and configurations
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def read_from_kafka_topic(consumer):

    messages = consumer.poll(timeout_ms=1000)
    data = []

    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data

votes_per_candidate_consumer = create_kafka_consumer('votes_per_candidate')


# read static candidates data
candidates_data = pd.read_csv('data/candidates.csv')

candidates_dict = OrderedDict({})
leading_candidate_id = None
leading_candidate_photo_url = ''
majority_votes = -1


for row in candidates_data[['candidate_id','candidate_name','party']].itertuples():

    candidates_dict[row[1]] = {'name':row[2], 'party':row[3], 'votes':0}

candidates_list = [ value['name'] for key, value in candidates_dict.items()]

app = Dash()

external_stylesheets = ['https://cdn.tailwindcss.com']

app = Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = [
        html.Div( children=[
            dcc.Interval(
            id='interval-component',
            interval=2*1000, # in milliseconds
            n_intervals=0
        )
        ]),
        html.Div(className='row', children='Realtime Voting feed',style={'textAlign':'center', 'fontSize':30, 'backgroundColor':'lightblue'}),
        html.Div(
            style={'display':'flex', 'backgroundColor':'black', 'justifyContent':'center'},
            children=[html.Div( style={'display':'flex', 'width':'80%', 'backgroundColor':'white' ,'justifyContent':'center'},children=[
            html.Div(className='',id='live-leading-candidate',style={'textAlign':'center', 'width':'40%'}),
            dcc.Graph(className='',id='live-votes-per-candidate-bar-graph')
        ])]
        ),
        html.Div(
            style={'display':'flex', 'backgroundColor':'black', 'justifyContent':'center'},
            children=[html.Div( style={'display':'flex', 'width':'80%', 'backgroundColor':'white' ,'justifyContent':'center'},children=[
            dcc.Graph(className='',id='live-votes-by-state-graph'),
            dcc.Graph(className='',id='live-votes-by-gender-graph')
        ])]
        )
        
    ]




@callback([Output('live-votes-per-candidate-bar-graph','figure'), Output('live-leading-candidate','children')],Input('interval-component','n_intervals'))
def update_votes_per_candidate(n):

    global majority_votes
    global leading_candidate_id
    global leading_candidate_photo_url

    data = read_from_kafka_topic(votes_per_candidate_consumer)
    results = pd.DataFrame(data)

    # update the total votes in candidates dict data to keep track of total votes for each candidate
    if len(results) > 0:
        for row in results[['candidate_id','total_votes', 'photo_url']].itertuples():

            candidate_id = row[1]
            votes = row[2]
            candidates_dict[candidate_id]['votes'] = votes

            if votes > majority_votes:
                majority_votes = votes
                leading_candidate_id = candidate_id
                leading_candidate_photo_url = row[3]
        
    votes_data = [value['votes'] for key, value in candidates_dict.items()]

    fig = go.Figure(
        data = go.Bar(
            x = candidates_list,
            y = votes_data,
        ),
        layout=go.Layout(
        title=go.layout.Title(text="Votes per candidate"),
    )
    )

    fig.update_layout(
        title_x=0.5,
        title_y=0.85,
        xaxis_title='Candidates',
        yaxis_title='Total Votes',
        font=dict(
            # family="Courier New, monospace",
            # size=18,
            # color="RebeccaPurple"
        )
        )

    leading_candidate_component = html.Div(style={'marginTop':'80px'},children= [
        html.Img(src=leading_candidate_photo_url, width='50%'),
        html.H4("Leading Candidate : "+ candidates_dict[leading_candidate_id]['name']),
        html.H3("Party : " + candidates_dict[leading_candidate_id]['party']),
        
    ])


    return [fig, leading_candidate_component]



if __name__ == '__main__':
    app.run(debug=True)
