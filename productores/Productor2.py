import math

def productor_sensor_2():
    sensor_id = "SENSOR-02"
    ubicacion = "Ala_Sur_Almacen"
    temp_media = 20.0
    amplitud = 5.0  # Varía entre 15°C y 25°C

    print(f"Iniciando {sensor_id}...")

    while True:
        ahora = datetime.now()
        
        # Convertir la hora del día a radianes para un ciclo de 24h
        # (Hora actual / 24) * 2π
        segundos_del_dia = ahora.hour * 3600 + ahora.minute * 60 + ahora.second
        fase = (segundos_del_dia / 86400) * (2 * math.pi)
        
        # Simular ciclo: El pico máximo será a las 12:00-14:00 aprox.
        # Ajustamos con -pi/2 para que el mínimo sea de madrugada
        lectura = round(temp_media + amplitud * math.sin(fase - math.pi/2), 2)
        
        mensaje = {
            "id_sensor": sensor_id,
            "temperatura": lectura,
            "timestamp ahora": ahora.isoformat(),
            "ubicacion": ubicacion,
            "tipo_comportamiento": "ciclo_diario_estable"
        }
        
        print(f"[ENVIO] {json.dumps(mensaje)}")
        time.sleep(5)