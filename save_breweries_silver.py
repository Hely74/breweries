import pandas as pd
import os

def load_bronze(bronze_path):
    """Carrega os dados da camada Bronze."""
    print(f"Carregando dados da Bronze: {bronze_path}")
    df = pd.read_json(bronze_path)
    print(f"Dados carregados: {len(df)} registros")
    return df

def preprocess_silver(df):
    """Remove duplicatas, seleciona colunas importantes e padroniza dados."""
    print("Removendo duplicatas...")
    df = df.drop_duplicates()

    # Seleciona colunas importantes
    df = df[['id', 'name', 'brewery_type', 'city', 'state', 'country', 'website_url']]
    df.columns = [col.lower() for col in df.columns]

    # Métricas de dados faltantes
    missing_city = df['city'].isna().sum()
    missing_state = df['state'].isna().sum()
    missing_country = df['country'].isna().sum()
    missing_website = df['website_url'].isna().sum()
    print(f"Registros sem cidade: {missing_city}")
    print(f"Registros sem estado: {missing_state}")
    print(f"Registros sem país: {missing_country}")
    print(f"Registros sem website: {missing_website}")

    # Substitui NaN por "No information"
    cols_to_fill = ['city', 'state', 'country', 'website_url']
    df[cols_to_fill] = df[cols_to_fill].fillna("No information")

    # Ajusta tipos de dados
    for col in df.columns:
        df[col] = df[col].astype(str)

    return df

def save_silver(df, silver_path):
    """Salva os dados processados na camada Silver, particionando por estado."""
    os.makedirs(silver_path, exist_ok=True)

    # Particiona por estado
    for state, group in df.groupby('state'):
        state_path = os.path.join(silver_path, f"state={state}")
        os.makedirs(state_path, exist_ok=True)
        group.to_parquet(os.path.join(state_path, "breweries.parquet"), index=False)
    
    print(f"Dados da Silver processados e particionados por estado em: {silver_path}")

if __name__ == "__main__":
    bronze_path = "data/bronze/breweries_raw.json"
    silver_path = "data/silver/"

    # Pipeline
    df_bronze = load_bronze(bronze_path)
    df_silver = preprocess_silver(df_bronze)
    save_silver(df_silver, silver_path)

    # Visualiza o dataframe final (opcional em produção)
    display(df_silver)