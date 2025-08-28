
# Airflow con Docker Compose + Makefile

Este proyecto configura un entorno local de **Apache Airflow** con **PostgreSQL** y **Redis** usando `docker compose`.  
La gestión se simplifica con un **Makefile**, que automatiza la creación del archivo `.env` (para claves y contraseñas) y el ciclo de vida de los contenedores.

---

##  Servicios incluidos

- **Postgres** → base de datos para Airflow.  
- **Redis** → backend de mensajes para Celery.  
- **Airflow Webserver** → interfaz web (puerto 8080).  
- **Airflow Scheduler** → planifica la ejecución de DAGs.  
- **Airflow Worker** → ejecuta las tareas en paralelo.  
- **Airflow Triggerer** → maneja triggers asíncronos.  
- **Airflow Init** → inicializa la base de datos y crea el usuario admin.  
- **Airflow CLI** → cliente para depuración y ejecución de comandos.  
- **Flower (opcional)** → monitor de Celery en puerto 5555.

---

##  Requisitos previos

- [Docker](https://docs.docker.com/get-docker/)  
- [Docker Compose v2](https://docs.docker.com/compose/install/)  
- [GNU Make](https://www.gnu.org/software/make/)

Verifica la instalación:

```bash
docker --version
docker compose version
make --version
```

## Variables de entorno

El archivo `.env` se crea automáticamente con:

```dotenv
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

 al ejecutar `make env`.  
- **Usuario Web** (`_AIRFLOW_WWW_USER_USERNAME`) → por defecto `airflow`.  
- **Password Web** (`_AIRFLOW_WWW_USER_PASSWORD`) → por defecto `airflow`.  

## Uso del Makefile

| Comando       | Descripción |
|---------------|-------------|
| `make env`    | Crea/actualiza el archivo `.env` con claves y contraseñas. |
| `make up`     | Levanta el stack en segundo plano (`docker compose up -d`). |
| `make down`   | Detiene los contenedores sin borrar volúmenes. |
| `make logs`   | Muestra los logs del **webserver**. |
| `make ps`     | Muestra el estado de los servicios. |
| `make restart`| Reinicia los servicios. |
| `make clean`  | Detiene y elimina los volúmenes (se pierde la base de datos). |
| `make destroy`| Igual que `clean` pero también borra el `.env`. |

Accede a  http://localhost:8080 


# Analisis y exploracion de los datos.
## Crear y activar env usando python.
```bash
python3 -m venv env
source env/bin/activate
pip install -r requiremnts
```
# Desactivar entorno virtual 
```bash
deactivate
```
## notebook analisis y entenidmiento de los datos.
### Instalar jupyter 
```bash
# Instalar jupyter si no lo tienes
pip install notebook ipython pandas chardet
```
Dentro de la carpeta data/ encontrara el notebook donde miro que es necesario hacer para identificar como limpair los datos deacuerdo a la informacion obtenida de lso archivos csv.
```exploracion_dataset.ipnb```

