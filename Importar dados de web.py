# -*- coding: utf-8 -*-
"""
Created on Mon May 16 10:04:45 2022

@author: lucas
"""

import pandas as pd
from selenium import webdriver
import time
import pyodbc
from datetime import date, timedelta, datetime


# Conectando no driver para acesso a pagina Web:
browser = webdriver.Edge(
    executable_path="C:/Users/lucas/Downloads/edgedriver_win64/msedgedriver.exe")

browser.get(
    "https://app.akna.com.br/site/montatela.php?t=acesse&header=n&footer=n&lang=pt-br")

time.sleep(2)


# Efetuando o login:
username = browser.find_element_by_name("cmpLogin")
password = browser.find_element_by_name("cmpSenha")

username.send_keys("Usuario!")
password.send_keys("Senha!")

time.sleep(2)

login = browser.find_element_by_tag_name('button').click()

time.sleep(2)


# Acesso aos relatórios:
browser.get(
    "https://app.akna.com.br/passport/montatela.php?t=emkt_login&modulo=LGN")

time.sleep(2)


# Periodo do relatório - MAX=90
intervalo = 90
dataf = date.today() - timedelta(days=1)
datafinal = '{:02d}%2F{:02d}%2F{}'.format(dataf.day, dataf.month, dataf.year)
datai = date.today() - timedelta(days=intervalo)
datainicial = '{:02d}%2F{:02d}%2F{}'.format(datai.day, datai.month, datai.year)

browser.get(
    f"https://app.akna.com.br/emkt/painel/montatela.php?t=estatisticas_campanha&modulo=CAM&data_inicial={datainicial}&data_final={datafinal}&tipo=pontual_recorrente")

time.sleep(30)


# Extração dos dados:
tabela = pd.read_html(browser.page_source)
time.sleep(5)


# Saindo do driver:
browser.get("https://app.akna.com.br/passport/logout.php")
time.sleep(3)
browser.quit()


# Tratamento de dados para importação no Banco de dados:
# Juntar as tabelas:
aknaTb = pd.concat(tabela, axis=1, join='inner')

# Renomear colunas:
aknaTb.columns = ['Campanhas', 'DataEnvio', 'Enviados', 'Entregues', 'Aberturas', 'AbertasTotal', 'Cliques', 'CliquesUnicos', 'CliquesTotal',
                  'CTOR', 'CTR', 'NaoEntregues', 'Campanhas2', 'Divulgacoes', 'Visualizacoes', 'SociaisCliques', 'Indicacoes', 'Remocoes', 'Spam']

# Removendo colunas não utilizadas:
aknaTb.drop(columns=['Cliques', 'AbertasTotal', 'CTOR', 'CTR', 'Campanhas2', 'Divulgacoes',
            'Visualizacoes', 'SociaisCliques', 'Indicacoes', 'Remocoes'], inplace=True)

# Removendo linhas de sub-total
aknaTb = aknaTb.dropna(subset='DataEnvio')

# Removendo porcentagem de entrgues:
aknaTb['Entregues'] = aknaTb['Entregues'].apply(lambda x: x.split()[0])

# Ajuste tipo date
aknaTb['DataEnvio'] = aknaTb['DataEnvio'].apply(
    lambda x: datetime.strptime(x, '%d/%m/%Y').date())


# Inserindo no banco de dados:
# Tabela deve ser criada previamente no banco de dados.

# dados_conexao
server = 'Nome do servidor!'
database = 'Nome do banco de dados!'
username = 'Usuario!'
password = 'Senha!'

# Conexão
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server +
                      ';DATABASE='+database +
                      ';UID='+username +
                      ';PWD=' + password
                      )
cursor = cnxn.cursor()

# Inserção no banco de dados:
for index, row in aknaTb.iterrows():
    cursor.execute("""INSERT INTO [dbo].[Akna]
                     ( [Campanha]
                     , [DataEnvio]
                     , [Enviados]
                     , [Entregues]
                     , [Aberturas]
                     , [CliquesUnicos]
                     , [CliquesTotais]
                     , [NaoEntregues]
                     , [Spam])
                    values(?,?,?,?,?,?,?,?,?)""", row.Campanhas, row.DataEnvio, row.Enviados, row.Entregues, row.Aberturas, row.CliquesUnicos, row.CliquesTotal, row.NaoEntregues, row.Spam
                   )
    cnxn.commit()

cursor.close()
