import pandas as pd
import os
import glob

# Caminho da camada Silver particionada
silver_path = "data/silver/"

# Caminho de saída da camada Gold
gold_path = "data/gold/"
os.makedirs(gold_path, exist_ok=True)

# Lista para armazenar dataframes de cada estado
dfs = []

# Percorre cada pasta de estado na Silver
for state_folder in glob.glob(os.path.join(silver_path, "state=*")):
    # Lê todos os arquivos Parquet dentro da pasta do estado
    for file in glob.glob(os.path.join(state_folder, "*.parquet")):
        df = pd.read_parquet(file)
        dfs.append(df)

# Concatena todos os dataframes
df_silver_all = pd.concat(dfs, ignore_index=True)

# Cria agregação: quantidade de cervejarias por tipo e estado
df_gold = df_silver_all.groupby(['state', 'brewery_type']).agg(
    total_breweries=pd.NamedAgg(column='id', aggfunc='count')
).reset_index()

# Ordena por estado e tipo
df_gold = df_gold.sort_values(['state', 'brewery_type'])

# Salva camada Gold
output_file = os.path.join(gold_path, "breweries_gold.parquet")
df_gold.to_parquet(output_file, index=False)

print(f"Camada Gold salva em: {output_file}")

# Visualiza o dataframe Gold
display(df_gold)