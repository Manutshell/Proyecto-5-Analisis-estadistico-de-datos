# Cargar todas las librerías
import pandas as pd 

# Carga los archivos de datos en diferentes DataFrames
df_calls = pd.read_csv('D:/TRIPLETEN DISCO 2/datasets/megaline_calls.csv')
df_internet = pd.read_csv('D:/TRIPLETEN DISCO 2/datasets/megaline_internet.csv')
df_messages = pd.read_csv('D:/TRIPLETEN DISCO 2/datasets/megaline_messages.csv')
df_plans = pd.read_csv('D:/TRIPLETEN DISCO 2/datasets/megaline_plans.csv')
df_users = pd.read_csv('D:/TRIPLETEN DISCO 2/datasets/megaline_users.csv')

# Imprime la información general/resumida sobre el DataFrame de las tarifas
#print(df_plans.info())

# Imprime una muestra de los datos para las tarifas
#print(df_plans)

# [Agrega factores adicionales a los datos si crees que pudieran ser útiles.]

df_plans['seconds_included'] = df_plans['minutes_included']*60
#print(df_plans)



# Imprime la información general/resumida sobre el DataFrame de usuarios
#print(df_users.info())

# Imprime una muestra de datos para usuarios
#print(df_users.sample(n=5))

# [Corrige los problemas obvios con los datos basándote en las observaciones iniciales.]

df_users['reg_date'] = pd.to_datetime(df_users['reg_date'])

df_users['churn_date'] = pd.to_datetime(df_users['churn_date'])



# Imprime la información general/resumida sobre el DataFrame de las llamadas

#print(df_calls.info())

# Imprime una muestra de datos para las llamadas

#print(df_calls.sample(n=5))

# [Corrige los problemas obvios con los datos basándote en las observaciones iniciales.]

df_calls['call_date'] = pd.to_datetime(df_calls['call_date'])



# Imprime la información general/resumida sobre el DataFrame de los mensajes
#print(df_messages.info())

# Imprime una muestra de datos para los mensajes
#print(df_messages.sample(n=5))

# [Corrige los problemas obvios con los datos basándote en las observaciones iniciales.]
df_messages['message_date'] = pd.to_datetime(df_messages['message_date'])



# Imprime la información general/resumida sobre el DataFrame de internet
#print(df_internet.info())

# Imprime una muestra de datos para el tráfico de internet
#print(df_internet.sample(n=5))

# [Corrige los problemas obvios con los datos basándote en las observaciones iniciales.]
df_internet['session_date'] = pd.to_datetime(df_internet['session_date'])







# Agregar datos por usuario
# [Ahora que los datos están limpios, agrega los datos por usuario y por periodo para que solo haya un registro por usuario y por periodo. 
# Esto facilitará mucho el análisis posterior.]


#--------------------------------------------------------------------------------------------------------------------------
# Calcula el número de llamadas hechas por cada usuario al mes. Guarda el resultado.
hejemplo = df_calls.groupby(pd.Grouper(key='call_date', freq='M'))['duration'].sum()
agg = {'duration': 'sum', 'user_id': ''}

hejemplo2 = df_calls.groupby('user_id')['duration'].sum()

hejemplo3 = df_calls.pivot_table(index = 'user_id', columns = 'call_date', values = 'duration', aggfunc = 'sum')
# -------------------------------------------------------------------------------------------------------------------------



# Calcula el número de llamadas hechas por cada usuario al mes. Guarda el resultado.
df_calls['year_month'] = df_calls['call_date'].dt.to_period('M')

monthly_calls_per_user = df_calls.groupby(['user_id', 'year_month']).size().reset_index(name='call_count')
print(monthly_calls_per_user)


# Calcula la cantidad de minutos usados por cada usuario al mes. Guarda el resultado.

monthly_minutes_per_user = df_calls.groupby(['user_id', 'year_month'])['duration'].sum().reset_index()
print(monthly_minutes_per_user)


# Calcula el número de mensajes enviados por cada usuario al mes. Guarda el resultado.

df_messages['year_month'] = df_messages['message_date'].dt.to_period('M')
monthly_message_per_user = df_messages.groupby(['user_id', 'year_month']).size().reset_index(name='message_count')
print(monthly_message_per_user)



# Calcula el volumen del tráfico de Internet usado por cada usuario al mes. Guarda el resultado.
df_internet['year_month'] = df_internet['session_date'].dt.to_period('M')

monthly_mb_per_user = df_internet.groupby(['user_id', 'year_month'])['mb_used'].sum().reset_index()
print(monthly_mb_per_user)

