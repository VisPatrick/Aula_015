from datetime import datetime
import gc
import os
import polars as pl

ENDERECO_DADOS = r'../../dados/'
ENDERECO_BRONZE = r'../../bronze/'

try:
    print('Obtendo dados')

    inicio = datetime.now()

    df_bolsa_familia = None

    lista_arquivos = []

    # Arquivos CSVs do diretório
    lista_dir_arquivos = os.listdir(ENDERECO_DADOS)
    for arquivo in lista_dir_arquivos:
        if arquivo.endswith('.csv'):
            lista_arquivos.append(arquivo)

    print(lista_arquivos)

    for arquivo in lista_arquivos:
        print(f'Processando arquivo {arquivo}...')

        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

        if df_bolsa_familia is None:
            df_bolsa_familia = df
        else:
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])

        del df

        print(df_bolsa_familia.head())
        print(df_bolsa_familia.shape)
        print(f'Arquivo {arquivo} processado com sucesso!')

    # Conversões após concatenação
    print('Convertendo coluna Valor Parcela para float...')
    df_bolsa_familia = df_bolsa_familia.with_columns(
        pl.col('VALOR PARCELA').str.replace(',', '.').cast(pl.Float64)
    )

    # convertendo MÊS COMPETÊNCIA para string
    print('Convertendo coluna MÊS COMPETÊNCIA para string...')
    df_bolsa_familia = df_bolsa_familia.with_columns(
        pl.col('MÊS COMPETÊNCIA').cast(pl.Utf8)
    )

    #   convertendo MÊS REFERÊNCIA para string
    print('Convertendo coluna MÊS REFERÊNCIA para string...')
    df_bolsa_familia = df_bolsa_familia.with_columns(
        pl.col('MÊS REFERÊNCIA').cast(pl.Utf8)
    )

    print('\nDados dos DataFrames concatenados com sucesso!')
    print('Iniciando a gravação do arquivo Parquet...')

    # Criação do arquivo Parquet
    # df_bolsa_familia.write_parquet('D:/bronze/bolsa_familia_str_cache.parquet')
    df_bolsa_familia.write_parquet(ENDERECO_BRONZE + 'bolsa_familia_str_cache.parquet')

    print('\nBolsa Família Concatenado')
    print(df_bolsa_familia.head())
    print(df_bolsa_familia.shape)

    del df_bolsa_familia    
    gc.collect()

    fim = datetime.now()
    
    print(f'Tempo de execução: {fim - inicio}')
    print('Gravação do arquivo Parquet realizada com sucesso!')

except ImportError as e:
    print(f'Erro ao processar os dataframes: {e}')
