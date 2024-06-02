from dash import Dash, html, dcc, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from kafka import KafkaConsumer
import json

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

consumer = create_kafka_consumer('votes_per_candidate')


app = Dash()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = [
        html.Div([
            dcc.Interval(
            id='interval-component',
            interval=1*1000, # in milliseconds
            n_intervals=0
        )
        ]),
        html.Div(className='row', children='Realtime Voting feed',style={'textAlign':'center', 'fontSize':30, 'backgroundColor':'lightblue'}),
        html.Div(className='row', children=[
            html.Div(className='six columns',id='live-leading-candidate'),
            dcc.Graph(className='six columns',id='live-update-graph')

        ])
    ]




@callback([Output('live-update-graph','figure'), Output('live-leading-candidate','children')],Input('interval-component','n_intervals'))
def update_votes_per_candidate(n):

    data = read_from_kafka_topic(consumer)
    results = pd.DataFrame(data)

    # use all candidate names as x axis from static data
    # store previous values and update only the new value in graph 

    print("results >> \n", results)

    fig = px.bar(
        results, 'candidate_name', 'total_votes'
    )

        # Identify the leading candidate
    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    results = results[['candidate_id', 'candidate_name', 'party','total_votes']]
    results = results.reset_index(drop=True)

    leading_candidate_component = html.Div([
        html.H4(leading_candidate['candidate_name']),
        html.H3(leading_candidate['party'])
    ])


    return [fig, leading_candidate_component]



if __name__ == '__main__':
    app.run(debug=True)
