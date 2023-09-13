import pandas as pd

# Carga el archivo CSV en un DataFrame de pandas
df = pd.read_csv('./data/realestate.csv')

# Muestra los tipos de datos de cada columna
print(df.dtypes)
