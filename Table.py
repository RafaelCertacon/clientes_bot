import sqlalchemy
from sqlalchemy import Column, String, Integer, DateTime, DECIMAL, insert
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.declarative import declarative_base


engine = sqlalchemy.create_engine(
    'mssql+pyodbc://rafael.rocha:Certa%402024@192.168.0.26/Teste_CertaBot?driver=ODBC+Driver+17+for+SQL+Server', pool_size=1000, echo=True
)

engine2 = sqlalchemy.create_engine(
    'mssql+pyodbc://matheus.oliveira:Makron3254@192.168.0.96/Teste_Certabot?driver=ODBC+Driver+17+for+SQL+Server', pool_size=1000, echo=True
)

Base = declarative_base()

class datalote(Base):
    __tablename__ = 'c_fe_datalote_dagente_0110'
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
    NCM = Column(String(250))
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

    def __init__(self,
        ID,
        numeroChave,
        cUF,
        cNF,
        mod,
        xNome,
        CNPJ,
        dest_cnpj,
        dest_cpf,
        dest_xnome,
        nserieSAT,
        nCFe,
        dEmi,
        nItem,
        cProd,
        xProd,
        NCM,
        CFOP,
        qCom,
        uCom,
        vUnCom,
        vProd,
        vItem,
        icms_orig,
        icms_cst,
        pis_cst,
        pis_vBC,
        pis_pPIS,
        pis_vPIS,
        cofins_cst,
        cofins_vBC,
        cofins_pCOFINS,
        cofins_vCOFINS,
        fileName,
        data_processamento,
        nome_da_maquina,
    ):
        self.ID = ID
        self.numeroChave = numeroChave
        self.cUF = cUF
        self.cNF = cNF
        self.mod = mod
        self.xNome = xNome
        self.CNPJ = CNPJ
        self.dest_cnpj = dest_cnpj
        self.dest_cpf = dest_cpf
        self.dest_xnome = dest_xnome
        self.nserieSAT = nserieSAT
        self.nCFe = nCFe
        self.dEmi = dEmi
        self.nItem = nItem
        self.cProd = cProd
        self.xProd = xProd
        self.NCM = NCM
        self.CFOP = CFOP
        self.qCom = qCom
        self.uCom = uCom
        self.vUnCom = vUnCom
        self.vProd = vProd
        self.vItem = vItem
        self.icms_orig = icms_orig
        self.icms_cst = icms_cst
        self.pis_cst = pis_cst
        self.pis_vBC = pis_vBC
        self.pis_pPIS = pis_pPIS
        self.pis_vPIS = pis_vPIS
        self.cofins_cst = cofins_cst
        self.cofins_vBC = cofins_vBC
        self.cofins_pCOFINS = cofins_pCOFINS
        self.cofins_vCOFINS = cofins_vCOFINS
        self.fileName = fileName
        self.data_processamento = data_processamento
        self.nome_da_maquina = nome_da_maquina

class Banco_cliente(Base):
    __tablename__ = 'tabela_cliente'
    ID = Column(Integer, primary_key=True, autoincrement=True)
    numeroChave = Column(String(250))
    cUF = Column(Integer)
    cNF = Column(String(250))
    mod = Column(Integer)
    xNome = Column(String(250))
    CNPJ = Column(String(250))
    dest_cnpj = Column(String(250))
    dest_cpf = Column(String(250))
    dest_xnome = Column(String(250))
    nserieSAT = Column(String(250))
    nCFe = Column(String(250))
    dEmi = Column(String(8))
    nItem = Column(Integer)
    cProd = Column(Integer)
    xProd = Column(String(250))
    NCM = Column(String(250))
    CFOP = Column(Integer)
    qCom = Column(DECIMAL)
    uCom = Column(String(250))
    vUnCom = Column(DECIMAL)
    vProd = Column(DECIMAL)
    vItem = Column(DECIMAL)
    icms_cst = Column(String(250))
    icms_orig = Column(String(250))
    pis_cst = Column(String(10))
    pis_vBC = Column(DECIMAL)
    pis_pPIS = Column(DECIMAL)
    pis_vPIS = Column(DECIMAL)
    cofins_cst = Column(String(10))
    cofins_vBC = Column(DECIMAL)
    cofins_pCOFINS = Column(DECIMAL)
    cofins_vCOFINS = Column(DECIMAL)
    fileName = Column(String(250))
    data_processamento = Column(DateTime)
    nome_da_maquina = Column(String(250))

Base.metadata.create_all(engine)
Base.metadata.create_all(engine2)

Sessao = sessionmaker(bind=engine)
session = Sessao()

Sessao2 = sessionmaker(bind=engine2)
session2 = Sessao2()
