# Importa a biblioteca requests para fazer requisições HTTP à API
import requests
# Importa a biblioteca json para manipulação de dados JSON
import json
# Importa a biblioteca os para manipulação de arquivos e diretórios
import os
# Importa a biblioteca time para adicionar delays em caso de retry
import time

def fetch_all_breweries(per_page=50, max_retries=3, wait_seconds=120):
    """
    Função para buscar todas as cervejarias da API Open Brewery DB.

    Parâmetros:
    - per_page: número de registros por requisição (padrão 50)
    - max_retries: número máximo de tentativas em caso de erro
    - wait_seconds: tempo de espera entre tentativas em caso de erro (segundos)

    Retorna:
    - all_breweries: lista com todas as cervejarias obtidas da API
    """
    all_breweries = []  # Lista para armazenar todas as cervejarias
    page = 1            # Inicializa a página com 1 (primeira página da API)

    # Loop para percorrer todas as páginas enquanto houver dados
    while True:
        # Monta a URL da API com paginação
        url = f"https://api.openbrewerydb.org/v1/breweries?page={page}&per_page={per_page}"
        retries = 0  # Contador de tentativas em caso de erro

        # Loop de retry para tentar a requisição em caso de falha
        while retries < max_retries:
            try:
                # Faz a requisição HTTP GET à API
                response = requests.get(url)

                # Verifica se o status code da resposta é 200 (OK)
                if response.status_code == 200:
                    # Converte o conteúdo JSON da resposta em lista de dicionários
                    data = response.json()
                    # Sai do loop de retry porque a requisição foi bem-sucedida
                    break
                else:
                    # Incrementa o contador de retries
                    retries += 1
                    # Informa que houve erro e que será feita uma nova tentativa
                    print(f"Erro na requisição da página {page}: {response.status_code}")
                    print(f"Tentativa {retries} de {max_retries}. Aguardando {wait_seconds} segundos...")
                    # Aguarda antes de tentar novamente
                    time.sleep(wait_seconds)

            except requests.exceptions.RequestException as e:
                # Captura exceções de rede ou requisição
                retries += 1
                print(f"Exceção na requisição da página {page}: {e}")
                print(f"Tentativa {retries} de {max_retries}. Aguardando {wait_seconds} segundos...")
                # Aguarda antes de tentar novamente
                time.sleep(wait_seconds)
        else:
            # Se todas as tentativas falharem, interrompe o loop principal
            print(f"Falha na requisição da página {page} após {max_retries} tentativas. Abortando.")
            break

        # Se a lista de dados estiver vazia, significa que não há mais registros
        if not data:
            print("Fim dos dados alcançado.")
            break

        # Adiciona os registros obtidos nesta página à lista principal
        all_breweries.extend(data)
        # Informa quantos registros foram baixados nesta página
        print(f"Página {page} baixada com {len(data)} cervejarias.")
        # Passa para a próxima página
        page += 1

    # Retorna a lista completa de cervejarias
    return all_breweries

def save_bronze_data(data, output_path="data/bronze"):
    """
    Função para salvar os dados brutos da API na camada Bronze.

    Parâmetros:
    - data: lista de dicionários com os dados das cervejarias
    - output_path: caminho da pasta onde os dados serão salvos
    """
    # Cria a pasta caso ela não exista
    os.makedirs(output_path, exist_ok=True)
    # Define o caminho completo do arquivo JSON a ser salvo
    output_file = os.path.join(output_path, "breweries_raw.json")

    # Abre o arquivo para escrita em UTF-8 e salva os dados em JSON
    with open(output_file, "w", encoding="utf-8") as f:
        # json.dump salva a lista de dicionários em JSON legível
        json.dump(data, f, ensure_ascii=False, indent=2)

    # Imprime mensagem confirmando que os dados foram salvos
    print(f"Dados salvos na camada Bronze em {output_file}")

# Bloco principal do script
if __name__ == "__main__":
    # Chama a função para extrair todas as cervejarias da API
    breweries = fetch_all_breweries()
    # Chama a função para salvar os dados na camada Bronze
    save_bronze_data(breweries)