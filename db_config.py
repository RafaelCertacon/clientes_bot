import pandas as pd
import sqlalchemy

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, String, Integer, DateTime, DECIMAL
from sqlalchemy.orm import declarative_base, sessionmaker


engine = sqlalchemy.create_engine(
    'mssql+pyodbc://rafael.rocha:Certa%402024@192.168.0.26/Teste_CertaBot?driver=ODBC+Driver+17+for+SQL+Server', echo=True
)

Base = declarative_base()

class bd_cliente(Base):
    __tablename__ = 'bd_cliente'
    ID = Column(Integer, primary_key=True, autoincrement=True)
    numeroChave = Column(String(250))
    cUF = Column(Integer)
    cNF = Column(Integer)
    mod = Column(Integer)
    xNome = Column(String(250))
    CNPJ = Column(String(250))
    dest_cnpj = Column(String(250))
    dest_cpf = Column(String(250))
    dest_xnome = Column(String(250))
    nserieSAT = Column(Integer)
    nCFe = Column(Integer)
    dEmi = Column(Integer)
    nItem = Column(Integer)
    cProd = Column(Integer)
    xProd = Column(String(250))
    NCM = Column(Integer)
    CFOP = Column(Integer)
    qCom = Column(DECIMAL)
    uCom = Column(String(250))
    vUnCom = Column(DECIMAL)
    vProd = Column(DECIMAL)
    vItem = Column(DECIMAL)
    icms_cst = Column(String(250))
    icms_orig = Column(String(250))
    pis_cst = Column(Integer)
    pis_vBC = Column(DECIMAL)
    pis_pPIS = Column(DECIMAL)
    pis_vPIS = Column(DECIMAL)
    cofins_cst = Column(Integer)
    cofins_vBC = Column(DECIMAL)
    cofins_pCOFINS = Column(DECIMAL)
    cofins_vCOFINS = Column(DECIMAL)
    fileName = Column(String(250))
    data_processamento = Column(DateTime)
    nome_da_maquina = Column(String(250))

Base.metadata.create_all(engine)

Sessao = sessionmaker(bind=engine)
session = Sessao()

tabela = session.query(datalote).all()

df = pd.DataFrame(tabela, columns=['ID', 'numeroChave', 'cUF', 'cNF', 'mod', 'xNome', 'CNPJ', 'dest_cnpj', 'dest_cpf', 'dest_xnome', 'nserieSAT', 'nCFe', 'dEmi', 'nItem', 'cProd',
                                      'xProd', 'NCM', 'CFOP', 'qCom', 'uCom', 'vUnCom', 'vProd', 'vItem', 'icms_orig', 'icms_cst', 'pis_cst', 'pis_vBC', 'pis_pPIS', 'pis_vPIS',
                                      'cofins_cst', 'cofins_vBC', 'cofins_pCOFINS', 'cofins_vCOFINS', 'fileName', 'data_processamento', 'nome_da_maquina'])
print(f"SELECT DA TABELA: {df}")

