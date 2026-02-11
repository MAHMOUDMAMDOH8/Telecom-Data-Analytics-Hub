import pandas as pd
from sqlalchemy import create_engine

conn = create_engine('postgresql://postgres:7412@localhost:5432/telecom')
tables = ["users", "cell_sites", "devices", "agents"]

user_path = 'C:\\share folder\\Telecom Data Analytics Hub\\source\\telecom-generator\\DIM_USER.json'
cell_sites_path = 'C:\\share folder\\Telecom Data Analytics Hub\\source\\telecom-generator\\dim_cell_site.json'
devise_path = "C:\\share folder\\Telecom Data Analytics Hub\\source\\telecom-generator\\DIM_DEVICE.json"
agent_path = "C:\\share folder\\Telecom Data Analytics Hub\\source\\telecom-generator\\dim_agent.json"

user_df = pd.read_json(user_path)
cell_sites_df = pd.read_json(cell_sites_path)
devise_df = pd.read_json(devise_path)
agent_df = pd.read_json(agent_path)


user_df.to_sql(tables[0], conn, if_exists='replace', index=False)
cell_sites_df.to_sql(tables[1], conn, if_exists='replace', index=False)
devise_df.to_sql(tables[2], conn, if_exists='replace', index=False)
agent_df.to_sql(tables[3], conn, if_exists='replace', index=False)