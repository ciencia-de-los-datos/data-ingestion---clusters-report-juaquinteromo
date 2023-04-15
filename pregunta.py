"""
Ingestión de data - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    data = pd.read_fwf("clusters_report.txt", skiprows=4, skipfooter=0, header = None)
    data.columns = ['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave', 'principales_palabras_clave']

    data['cluster'] = data['cluster'].fillna(method="ffill")
    data['principales_palabras_clave'] = data[['cluster','principales_palabras_clave']].groupby(['cluster'])['principales_palabras_clave'].transform(lambda x: ' '.join((x)))
    data = data.dropna()
    data = data.reset_index()
    del data["index"]
    data = data.replace(r'\s+', ' ', regex=True)
    data['principales_palabras_clave'] = data['principales_palabras_clave'].str.replace('.','')
    data['porcentaje_de_palabras_clave'] = data['porcentaje_de_palabras_clave'].str[:-2]
    data['porcentaje_de_palabras_clave'] = data['porcentaje_de_palabras_clave'].str.replace(',','.')
    data['porcentaje_de_palabras_clave'] = pd.to_numeric(data['porcentaje_de_palabras_clave'])
    data = data.astype({'principales_palabras_clave':'string'})

    return data
