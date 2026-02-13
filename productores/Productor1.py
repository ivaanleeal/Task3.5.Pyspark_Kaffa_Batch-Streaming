import time
import json
import random
from datetime import datetime

def productor_sensor_1():
    sensor_id = "SENSOR-01"
    ubicacion = "Ala_Norte_Almacen"
    temp_base = 22.0
    incremento_por_hora = 0.5
    segundos_por_hora = 3600
    
    start_time = time.time()

    print(f"Iniciando {sensor_id}...")
    
    while True:
        # Calcular tendencia: tiempo transcurrido en horas * 0.5
        horas_transcurridas = (time.time() - start_time) / segundos_por_hora
        tendencia = horas_transcurridas * incremento_por_hora
        
        # Temperatura final = Base + Tendencia + Ruido aleatorio
        lectura = round(temp_base + tendencia + random.uniform(-0.2, 0.2), 2)
        
        mensaje = {
            "id_sensor": sensor_id,
            "temperatura": lectura,
            "timestamp": datetime.now().isoformat(),
            "ubicacion": ubicacion,
            "tipo_comportamiento": "calentamiento_gradual"
        }
        
        print(f"[ENVIO] {json.dumps(mensaje)}")
        time.sleep(5)  # Envío cada 5 segundos para la simulación