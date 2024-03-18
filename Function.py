import pandas as pd
import dask.dataframe as dd
import sqlalchemy
import dask
import csv
import math

from Table import datalote, Banco_cliente, session2, engine2, session, engine, Base

def criar_csv_do_banco_26():
    # primeiro pegar os dados da tabela principal no banco 26
    tabela = session.query(datalote).all()
    data = []
    # passar por todas as linhas da tabela principal no banco 26 e criar um novo dataframe
    for linha in tabela:
        data.append((linha.numeroChave, linha.cUF, linha.cNF, linha.mod, linha.xNome, linha.CNPJ, linha.dest_cnpj, linha.dest_cpf,
                     linha.dest_xnome, linha.nserieSAT, linha.nCFe, linha.dEmi, linha.nItem, linha.cProd, linha.xProd, linha.NCM, linha.CFOP, linha.qCom,
                     linha.uCom, linha.vUnCom, linha.vProd, linha.vItem, linha.icms_orig, linha.icms_cst, linha.pis_cst, linha.pis_vBC, linha.pis_pPIS, linha.pis_vPIS,
                     linha.cofins_cst, linha.cofins_vBC, linha.cofins_pCOFINS, linha.cofins_vCOFINS, linha.fileName, linha.data_processamento, linha.nome_da_maquina))
    df = pd.DataFrame(data, columns=['numeroChave', 'cUF', 'cNF', 'mod', 'xNome', 'CNPJ', 'dest_cnpj', 'dest_cpf', 'dest_xnome', 'nserieSAT', 'nCFe', 'dEmi', 'nItem', 'cProd',
                                      'xProd', 'NCM', 'CFOP', 'qCom', 'uCom', 'vUnCom', 'vProd', 'vItem', 'icms_orig', 'icms_cst', 'pis_cst', 'pis_vBC', 'pis_pPIS', 'pis_vPIS',
                                      'cofins_cst', 'cofins_vBC', 'cofins_pCOFINS', 'cofins_vCOFINS', 'fileName', 'data_processamento', 'nome_da_maquina'])

    #pegar os dados do datafrane e transformar em csv
    df.to_csv('Nota_Fiscal/tabela_csv.csv', index=None)

#Leitura da tabela e dividir os dados da tabela
def particionar_arquivo_principal(parts, filename):
    ddf = dd.read_csv(filename, encoding='latin1', sep=',',  escapechar='\\', dtype={
        'numeroChave': 'str',
        'cNF': 'str',
        'xNome': 'str',
        'CNPJ': 'str',
        'NCM': 'str',
        'dest_cnpj': 'str',
        'dest_cpf': 'str',
        'dest_xnome': 'str',
        'nserieSAT': 'str',
        'nCFe': 'str',
        'dEmi': 'str',
        'uCom': 'str',
        'xProd': 'str',
        'icms_cst': 'str',
        'icms_orig': 'str',
        'pis_cst': 'str',
        'cofins_cst': 'str',
        'fileName': 'str',
        'nome_da_maquina': 'str',
    })
    #Pegar a participação e fazer um loop FOR pra pegar um por um e add em uma pasta dividia em 3
    partitions = ddf.repartition(npartitions=parts)
    for i, partition in enumerate(partitions.to_delayed()):
        partition_df = partition.compute()
        partition_filename = f'Arquivos_tabela/partition_{i}.csv'
        partition_df.to_csv(partition_filename, index=False, quoting=csv.QUOTE_NONE,  escapechar='\\')

    print("Arquivos divididos e salvos com sucesso!")

#Remove uma coluna e as barras ( / ) que separa de uma dado ao outro
def read_remove_escape_add_to_sql(num_partitions):
    for i in range(num_partitions):
        filename = f'Arquivos_tabela/partition_{i}.csv'
        column_names = ['numeroChave', 'cUF', 'cNF', 'mod', 'xNome', 'CNPJ', 'dest_cnpj', 'dest_cpf', 'dest_xnome',
                        'nserieSAT', 'nCFe', 'dEmi', 'nItem', 'cProd',
                        'xProd', 'NCM', 'CFOP', 'qCom', 'uCom', 'vUnCom', 'vProd', 'vItem', 'icms_orig', 'icms_cst',
                        'pis_cst', 'pis_vBC', 'pis_pPIS', 'pis_vPIS',
                        'cofins_cst', 'cofins_vBC', 'cofins_pCOFINS', 'cofins_vCOFINS', 'fileName',
                        'data_processamento', 'nome_da_maquina']

        df = pd.read_csv(filename, names=column_names, skiprows=1,  escapechar='\\', dtype={
        'numeroChave': 'str',
        'cNF': 'str',
        'xNome': 'str',
        'CNPJ': 'str',
        'NCM': 'str',
        'dest_cnpj': 'str',
        'dest_cpf': 'str',
        'dest_xnome': 'str',
        'nserieSAT': 'str',
        'nCFe': 'str',
        'dEmi': 'str',
        'uCom': 'str',
        'xProd': 'str',
        'icms_cst': 'str',
        'icms_orig': 'str',
        'pis_cst': 'str',
        'cofins_cst': 'str',
        'fileName': 'str',
        'nome_da_maquina': 'str',
    })#Add na tabela (INSERT)
        df.to_sql('tabela_cliente',  con=engine2, if_exists='append', index=None)

        print(f"Data from {filename} added to SQL database.")
