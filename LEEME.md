# Laboratorio BDP v7 en la nube

Es el mismo laboratorio de la versión portable (HDFS, Spark 4.1, Sedona, Elasticsearch y JupyterLab), corriendo en GitHub Codespaces. No necesitas instalar nada en tu computadora.

## Primeros pasos

1. Espera a que termine la preparación. La primera vez tarda unos 5 minutos, porque descarga el laboratorio.
2. En la **Terminal** (abajo), revisa qué está encendido:

   ```bash
   python3 bdp_start.py status
   ```

3. Carga los datos de práctica (Aguascalientes) en HDFS:

   ```bash
   python3 bdp_start.py datos
   ```

4. Abre JupyterLab desde la pestaña **Puertos**, puerto **8888**. Entra sin token. Tus cuadernos están en `notebooks/`.

## Comandos

| Para | Comando |
|---|---|
| Ver qué está encendido y las direcciones | `python3 bdp_start.py status` |
| Curso **UP** (HDFS + Jupyter + Elasticsearch) | `python3 bdp_start.py up` |
| Curso **UNIR** (HDFS + Jupyter + Kafka) | `python3 bdp_start.py unir` |
| Cargar los datos de práctica en HDFS | `python3 bdp_start.py datos` |
| Verificar que todo funciona (evidencia) | `python3 bdp_start.py check` |
| Apagar todo limpio | `python3 bdp_start.py stop` |

En la terminal ya funcionan `hdfs dfs -ls /`, `spark-submit`, `python` y `curl -X GET "http://localhost:9200/_cat/indices?v"`.

## Cuida tus horas gratis

- Esta máquina tiene 4 núcleos. Una hora de uso consume 4 de tus horas-núcleo: 120 al mes con una cuenta gratis, 180 con GitHub Student Pack.
- Cuando termines, cierra la pestaña. El codespace se detiene solo a los 30 minutos sin uso y tu trabajo se conserva.

Si algo falla, copia la salida de `python3 bdp_start.py check` y envíala por Blackboard.

— Dr. Abel Coronado
