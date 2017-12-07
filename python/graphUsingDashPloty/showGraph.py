import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import plotly.graph_objs as go
import pandas as pd
import sys
import plotly.plotly as py
app = dash.Dash()
dcc._css_dist[0]['relative_package_path'].append('mycss.css')
# call the data fromn csv

dcc._css_dist[0]['relative_package_path'].append('mycss.css')
# declare dictionary of values
values_dict={1:'MaskRemaining',
	     2:'BxxCD',
	     3:'ByyCD',
	     4:'XXCD',
	     5:'xxxDepth',
	     6:'RatioBxxCD_xxx',
}

app = dash.Dash()

df = pd.read_csv('MHxxx.csv')
dfR= pd.read_csv('MHyyy.csv')

df["MaskRemaining"]=df["MaskRemaining"].fillna(0.0).astype(int)
df["BxxCD"]=df["BxxCD"].fillna(0.0).astype(int)
df["ByyCD"]=df["ByyCD"].fillna(0.0).astype(int)
df["DeltaCD"]=df["XXCD"].fillna(0.0).astype(int)
df["xxxDepth"] = df["xxxDepth"].fillna(0.0).astype(int)
df["RatioBxxCD_xxx"] = pd.to_numeric(df.BxxCD)/pd.to_numeric(df.xxxDepth)

print " Lets look at the difference in recipes"

print " Printing distinct values for Recipe to show in the graph"
dfn =pd.unique(dfR[list(dfR.columns.values)].values)
ff= []
for n in dfn:
        ff.append(list(n))
gg1=pd.DataFrame(ff,columns=dfR.columns.values)
#print gg1
graph_options = ['bar','scatter','line']

runNum=['R01','R02']
def generate_table(dataframe):
    max_rows = len(dataframe.index)  
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))],	
	style={
                  'linecolor': 'rgb(205, 12, 24)' 
                }
    )


# start tab app


app.scripts.config.serve_locally = True

# toggle here to show the appropriate location for tab 
vertical = True
#askRemaining,BxxCD,ByyCD,XXCD,xxxDepth
if not vertical:
    app.layout = html.Div([
        dcc.Tabs(
            tabs=[
                {'label': 'MaskRemaining', 'value': 1},
                {'label': 'BxxCD', 'value': 2},
                {'label': 'ByyCD', 'value': 3},
                {'label': 'XXCD', 'value': 4},
		{'label': 'xxxDepth','value':5},
		{'label': 'Ratio of BxxCD & xxx','value':6},
            ],
            value=3,
            id='tabs',
            vertical=vertical
        ),
        html.Div(id='tab-output')
    ], style={
        'width': '80%',
        'fontFamily': 'Sans-Serif',
        'margin-left': 'auto',
        'margin-right': 'auto'
    })

else:
    app.layout = html.Div([
        html.Div(
            dcc.Tabs(
                tabs=[
                {'label': 'MaskRemaining', 'value': 1},
                {'label': 'BxxCD', 'value': 2},
                {'label': 'ByyCD', 'value': 3},
                {'label': 'XXCD', 'value': 4},
                {'label': 'xxxDepth','value':5},
                {'label': 'Ratio of BxxCD & xxx','value':6},
		],
                value=3,
                id='tabs',
                vertical=vertical,
                style={
                    'height': '100vh',
                    'borderRight': 'thin lightgrey solid',
                    'textAlign': 'left'
                }
            ),
            style={'width': '20%', 'float': 'left'}
        ),
	html.Div([
    dcc.RadioItems(
        id='graph-opn',
        options=[{'label': k, 'value': k} for k in graph_options],
        value='bar'
    ), html.Hr(),]),
        html.Div(
            html.Div(id='tab-output'),
            style={'width': '80%', 'float': 'right'}
        )
    ], style={
        'fontFamily': 'Sans-Serif',
        'margin-left': 'auto',
        'margin-right': 'auto',
    })

@app.callback(Output('tab-output', 'children'), [Input('tabs', 'value'),Input('graph-opn', 'value')])
def update_graph(value,graph_opn_value):
   
	if graph_opn_value == 'bar':
		data= [
                	go.Bar(
                          x=df[df['RunNo'] == i]['Location'],
                          y=df[df['RunNo'] == i][values_dict[value]],
                          text=df['RunNo'],
				name=i
                           ) for i in df.RunNo.unique()
               ]

	elif graph_opn_value == 'line':
		data= [
                go.Scatter(
                          x=df[df['RunNo'] == i]['Location'],
                          y=df[df['RunNo'] == i][values_dict[value]],
                          text=df['RunNo'],
                          mode='lines_markers',
                          opacity=0.7,
                          marker={
                                 'size': 15,
                                 'line': {'width': 0.5, 'color': 'white'}
                                 },
                                 name=i
                           ) for i in df.RunNo.unique()
               ]

	else:
		data= [
		go.Scatter(
                          x=df[df['RunNo'] == i]['Location'],
                          y=df[df['RunNo'] == i][values_dict[value]],
                          text=df['RunNo'],
                          mode='markers',
                          opacity=0.7,
                          marker={
                                 'size': 15,
                                 'line': {'width': 0.5, 'color': 'white'}
                                 },
                                 name=i
                           ) for i in df.RunNo.unique()
               ]


	return html.Div([
        dcc.Graph(
            id='graph',
            figure={
                'data': data,
                'layout': {
                    'margin': {
                        'l': 30,
                        'r': 0,
                        'b': 30,
                        't': 0
                    },
                    'dash' :'dot',
                    'text':df['RunNo'],
                    'mode': 'markers',
                    'opacity' :'0.5',
                    'marker':{'size': 15,
                              'line': {'width': 0.5}
                              },

                    'legend': {'x': 0, 'y': 1}
                }
            }
        ),
        html.H4(children='Distinct values for Recipe to show in the graph'),
                    generate_table(gg1),
    ])
    




#dcc._css_dist[0]['relative_package_path'].append('/Users/20163/anaconda/lib/python2.7/site-packages/dash_core_components/mycss1.css')
app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})
if __name__ == '__main__':
    app.run_server(debug=True)
