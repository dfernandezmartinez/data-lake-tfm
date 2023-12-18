from prophet import Prophet
import pandas as pd
import matplotlib.pyplot as plt

# Cargar datos
df = pd.read_csv('export.csv') 
df.rename(columns={'timestamp': 'ds', 'value': 'y'}, inplace=True)
df['ds'] = pd.to_datetime(df['ds'])

# Filtrar para obtener los primeros días
fecha_maxima = df['ds'].min() + pd.Timedelta(days=28)
df_filtrado = df[df['ds'] <= fecha_maxima]

# Ajustar el modelo con los datos filtrados
m = Prophet(weekly_seasonality=20)
m.fit(df_filtrado)

# Crear un dataframe para las predicciones
future = m.make_future_dataframe(periods=14*24, freq='H')
forecast = m.predict(future)

# Combinar las predicciones con los datos originales
df_pred = df.merge(forecast[['ds', 'yhat']], on='ds', how='left')
df_pred['diff'] = df_pred['y'] - df_pred['yhat']

# Detectar anomalías: mediciones consecutivas que estén a una distancia de <threshold> durante una ventana de tiempo de <time_window_seconds> o más
def detect_anomalies(df, threshold=20, time_window_seconds=86400):
    anomalies = []
    anomaly_flag = False
    for i, row in df.iterrows():
        if abs(row['diff']) > threshold:
            if not anomaly_flag:
                anomaly_flag = True
                start = row['ds']
        else:
            if anomaly_flag:
                anomaly_flag = False
                end = df.iloc[i - 1]['ds']
                if (end - start).total_seconds() >= time_window_seconds: 
                    anomalies.append((start, end))
    return anomalies

anomalies = detect_anomalies(df_pred, 3, 3600*4)

# Visualizar las anomalías en el gráfico
fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(df_pred['ds'], df_pred['y'], label='Real')
ax.plot(df_pred['ds'], df_pred['yhat'], label='Predicción')

for start, end in anomalies:
    ax.axvspan(start, end, color='red', alpha=0.3)

ax.legend()
plt.show()
