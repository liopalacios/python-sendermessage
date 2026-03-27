from flask import Flask, request, jsonify
import redis
import json
from flask_cors import CORS
import requests
import re
import qrcode
from io import BytesIO 
from base64 import b64encode
from uuid import uuid4
from threading import Thread, Lock
import jwt
import time
import random
from datetime import datetime, timedelta, timezone

redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

app = Flask(__name__)
# =================================================================
# CONFIGURACIÓN
# =================================================================

# La URL de tu servidor Baileys (asegúrate de que esté corriendo)
BAILEYS_API_URL = "http://localhost:3000/send"

# El código de país que usarás.
# EJEMPLO: Si todos tus números son de Perú, usarías '51'
# Si los números YA incluyen el código de país, déjalo vacío o manéjalo con lógica.
CODIGO_PAIS = "51" 
JWT_SECRET_KEY = "xJvOFRengB9iMGoCtTH0yDV6wL45ZuWN"

FIXED_USERNAME = "admin"
FIXED_PASSWORD = "certifact123"
TOKEN_EXPIRATION_MINUTES = 30
JOBS = {}

from functools import wraps
# =================================================================
# FUNCIÓN DE ENVÍO
# =================================================================

def scheduler_loop():
     # Al iniciar, recuperar jobs colgados
    recover_stuck_jobs()
    print("⏰ Scheduler iniciado, revisando la cola cada 25 segundos...")
    while True:
        try:
            # Verificar si hay un job activo
            active_job = check_active_job()
            
            if not active_job:
                # Tomar el primer job de la cola (excluyendo placeholder)
                job_ids = redis_client.zrange("jobs_queue", 0, -1)
                # Filtrar placeholder
                real_jobs = [j for j in job_ids if j != "__PLACEHOLDER__"]
                
                if real_jobs:
                    job_id = real_jobs[0]
                    print(f"📦 Procesando job: {job_id}")
                    process_single_job(job_id)
                else:
                    # No hay jobs reales
                    pass
            else:
                print(f"⏳ Ya hay un job activo: {active_job}")
                
        except Exception as e:
            print("❌ Scheduler error:", e)
            import traceback
            traceback.print_exc()
            
        time.sleep(25)

# Para limpiar el job stuck manualmente
def reset_stuck_job(job_id):
    """Resetea un job stuck manualmente"""
    job_data_raw = redis_client.get(f"job:{job_id}")
    if job_data_raw:
        job_data = json.loads(job_data_raw)
        job_data["status"] = "pending"
        job_data.pop("started_at", None)
        redis_client.set(f"job:{job_id}", json.dumps(job_data))
        # Eliminar el lock si existe
        redis_client.delete(f"lock:{job_id}")
        print(f"✅ Job {job_id} reseteado a pending")
        return True
    return False

# Ejecutar para limpiar el job stuck:
reset_stuck_job("5272c22a-f911-44f7-9756-afc208bfe232")

def check_active_job():
    """Verifica si hay algún job en estado 'processing'"""
    # Obtener todos los jobs en cola
    lock_keys = redis_client.keys("lock:*")
    if lock_keys:
        for lock_key in lock_keys:
            job_id = lock_key.replace("lock:", "")
            # Verificar que el job realmente existe
            job_key = f"job:{job_id}"
            if redis_client.exists(job_key):
                # Verificar que es string antes de hacer GET
                if redis_client.type(job_key) == 'string':
                    return job_id
    
    # Verificar jobs en estado processing
    all_job_keys = redis_client.keys("job:*")
    # Filtrar solo las keys que son jobs (no las de contacts)
    job_keys = [key for key in all_job_keys if not key.endswith(":contacts")]
    
    for key in job_keys:
        job_id = key.replace("job:", "")
        
        # Verificar tipo antes de GET
        if redis_client.type(key) != 'string':
            continue
            
        job_data_raw = redis_client.get(key)
        if job_data_raw:
            try:
                job_data = json.loads(job_data_raw)
                if job_data.get("status") == "processing":
                    # Verificar si realmente está en la cola
                    if redis_client.zscore("jobs_queue", job_id):
                        return job_id
            except:
                continue
    
    return None


def process_single_job(job_id):
    if job_id == "__PLACEHOLDER__":
        return
    
    lock_key = f"lock:{job_id}"

    # evitar doble ejecución
    if not redis_client.setnx(lock_key, 1):
        return
    
    try:
        job_key = f"job:{job_id}"
        
        # Verificar que la key existe y es string
        if not redis_client.exists(job_key):
            print(f"⚠️ Job {job_id} no existe, eliminando de cola...")
            redis_client.zrem("jobs_queue", job_id)
            return
            
        key_type = redis_client.type(job_key)
        if key_type != 'string':
            print(f"⚠️ Job {job_id} es de tipo {key_type}, eliminando de cola...")
            redis_client.zrem("jobs_queue", job_id)
            return
        
        job_data = json.loads(redis_client.get(job_key))
        message = job_data["message"]

        contacts_key = f"job:{job_id}:contacts"
        
        # Verificar que la key de contactos existe
        if not redis_client.exists(contacts_key):
            print(f"⚠️ Job {job_id} no tiene contactos, marcando como completado...")
            job_data["status"] = "completed"
            job_data["finished_at"] = datetime.now().isoformat()
            redis_client.set(job_key, json.dumps(job_data))
            redis_client.zrem("jobs_queue", job_id)
            return
        
        # Obtener SOLO contactos pendientes (estado = 0)
        all_contacts = redis_client.lrange(contacts_key, 0, -1)
        pending_indices = []
        
        for i, c in enumerate(all_contacts):
            try:
                obj = json.loads(c)
                if obj.get("estado", 0) == 0:
                    pending_indices.append((i, obj))
            except:
                continue
        
        # Si no hay pendientes, el job está completado
        if not pending_indices:
            job_data["status"] = "completed"
            job_data["finished_at"] = datetime.now().isoformat()
            redis_client.set(job_key, json.dumps(job_data))
            redis_client.zrem("jobs_queue", job_id)
            print(f"✅ Job completado (sin pendientes): {job_id}")
            return
        
        # Procesar hasta 30 pendientes
        batch = pending_indices[:30]
        processed = 0
        
        # Cambiar estado a processing
        if job_data.get("status") != "processing":
            job_data["status"] = "processing"
            redis_client.set(job_key, json.dumps(job_data))
        
        for idx, obj in batch:
            try:
                personalized = message.replace("#NOMBRE#", obj["nombre"])
                
                response = requests.post(
                    BAILEYS_API_URL,
                    json={
                        "message": personalized,
                        "contacto": {
                            "numero": obj["numero"],
                            "nombre": obj["nombre"]
                        }
                    },
                    timeout=10
                )
                
                if response.status_code == 200:
                    obj["estado"] = 1  # Enviado
                    print(f"✅ Enviado: {obj['numero']} - {obj['nombre']}")
                else:
                    obj["estado"] = 2  # Error
                    print(f"❌ Error API: {obj['numero']} - Status: {response.status_code}")
                    
            except Exception as e:
                obj["estado"] = 2  # Error
                print(f"❌ Excepción enviando a {obj['numero']}: {str(e)}")
            
            # Registrar log
            redis_client.rpush("logs", json.dumps({
                "job_id": job_id,
                "numero": obj["numero"],
                "nombre": obj["nombre"],
                "estado": obj["estado"],
                "fecha": datetime.now().isoformat()
            }))
            
            # Actualizar contacto en Redis
            redis_client.lset(contacts_key, idx, json.dumps(obj))
            
            processed += 1
            
            # Delay humano (excepto en el último)
            if processed < len(batch):
                delay = random.randint(10, 35)
                print(f"⏱️ Esperando {delay} segundos antes del próximo mensaje...")
                time.sleep(delay)
        
        # Verificar si después de este lote ya no quedan pendientes
        all_contacts_after = redis_client.lrange(contacts_key, 0, -1)
        remaining_pending = 0
        
        for c in all_contacts_after:
            try:
                if json.loads(c).get("estado", 0) == 0:
                    remaining_pending += 1
            except:
                pass
        
        total_contacts = len(all_contacts_after)
        sent = len([c for c in all_contacts_after if json.loads(c).get("estado") == 1])
        failed = len([c for c in all_contacts_after if json.loads(c).get("estado") == 2])
        
        print(f"📊 Job {job_id}: Total={total_contacts}, Enviados={sent}, Fallidos={failed}, Pendientes={remaining_pending}")
        
        # Si no quedan pendientes, marcar como completado
        if remaining_pending == 0:
            job_data["status"] = "completed"
            job_data["finished_at"] = datetime.now().isoformat()
            redis_client.set(job_key, json.dumps(job_data))
            redis_client.zrem("jobs_queue", job_id)
            print(f"✅ Job COMPLETADO: {job_id}")
        else:
            # Aún quedan pendientes, mantener en cola para próximo ciclo
            print(f"🔄 Job {job_id} aún tiene {remaining_pending} pendientes, continúa en cola")
            # Asegurar que está en la cola con timestamp actualizado
            redis_client.zadd("jobs_queue", {job_id: time.time()})
        
    except Exception as e:
        print(f"❌ Error procesando job {job_id}: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        redis_client.delete(lock_key)

def recover_stuck_jobs():
    """
    Recupera jobs que están marcados como 'processing' pero no tienen lock activo
    o que tienen estado 'pending' pero no están en la cola
    """
    print("🔍 Buscando jobs colgados...")
    
    # Buscar todos los jobs (solo las keys que terminan sin :contacts)
    all_job_keys = redis_client.keys("job:*")
    
    # Filtrar solo las keys que son jobs (no las de contacts)
    job_keys = [key for key in all_job_keys if not key.endswith(":contacts")]
    
    recovered = 0
    
    for key in job_keys:
        job_id = key.replace("job:", "")
        
        # Verificar el tipo de key antes de hacer GET
        key_type = redis_client.type(key)
        
        if key_type != 'string':
            print(f"⚠️ Key {key} es de tipo {key_type}, saltando...")
            continue
            
        try:
            job_data_raw = redis_client.get(key)
            if not job_data_raw:
                continue
                
            job_data = json.loads(job_data_raw)
            
            # Verificar si está en la cola
            in_queue = redis_client.zscore("jobs_queue", job_id) is not None
            
            # Obtener contactos
            contacts_key = f"job:{job_id}:contacts"
            contacts = redis_client.lrange(contacts_key, 0, -1) if redis_client.exists(contacts_key) else []
            
            # Contar estados
            total = len(contacts)
            pending = 0
            sent = 0
            failed = 0
            
            for c in contacts:
                try:
                    contact = json.loads(c)
                    estado = contact.get("estado", 0)
                    if estado == 1:
                        sent += 1
                    elif estado == 2:
                        failed += 1
                    else:
                        pending += 1
                except:
                    pending += 1
            
            # Caso 1: Job con pendientes = 0 pero no está completado
            if pending == 0 and total > 0 and job_data.get("status") != "completed":
                print(f"✅ Completando job que no se marcó: {job_id}")
                job_data["status"] = "completed"
                job_data["finished_at"] = datetime.now().isoformat()
                redis_client.set(key, json.dumps(job_data))
                redis_client.zrem("jobs_queue", job_id)
                recovered += 1
            
            # Caso 2: Job con pendientes > 0 pero no está en cola y no está processing
            elif pending > 0 and not in_queue and job_data.get("status") != "processing":
                print(f"🔄 Re-encolando job: {job_id} (tiene {pending} pendientes)")
                redis_client.zadd("jobs_queue", {job_id: time.time()})
                if job_data.get("status") == "completed":
                    job_data["status"] = "pending"
                    redis_client.set(key, json.dumps(job_data))
                recovered += 1
            
            # Caso 3: Job en estado 'processing' pero sin lock activo
            elif job_data.get("status") == "processing" and not in_queue:
                lock_key = f"lock:{job_id}"
                if not redis_client.exists(lock_key):
                    print(f"🔄 Recuperando job colgado en processing: {job_id}")
                    redis_client.zadd("jobs_queue", {job_id: time.time()})
                    recovered += 1
                    
        except Exception as e:
            print(f"❌ Error procesando job {job_id}: {str(e)}")
            continue
    
    # Limpiar placeholder si existe
    placeholder_score = redis_client.zscore("jobs_queue", "__PLACEHOLDER__")
    if placeholder_score is not None:
        print("🗑️ Eliminando placeholder de la cola...")
        redis_client.zrem("jobs_queue", "__PLACEHOLDER__")
    
    print(f"📊 Recuperados: {recovered} jobs")
    return recovered

def cleanup_stuck_jobs():
    """Limpia jobs que están en processing desde hace más de 5 minutos"""
    print("🔍 Buscando jobs stuck en processing...")
    real_jobs = safe_get_queue_jobs()
    
    for job_id in real_jobs:
        job_data_raw = redis_client.get(f"job:{job_id}")
        if job_data_raw:
            try:
                job_data = json.loads(job_data_raw)
                if job_data.get("status") == "processing":
                    started_at = job_data.get("started_at")
                    if started_at:
                        start_time = datetime.fromisoformat(started_at)
                        elapsed = (datetime.now() - start_time).total_seconds()
                        if elapsed > 300:  # 5 minutos
                            print(f"⚠️ Job stuck detectado: {job_id} (procesando por {elapsed:.0f}s)")
                            job_data["status"] = "pending"
                            job_data.pop("started_at", None)
                            redis_client.set(f"job:{job_id}", json.dumps(job_data))
                            print(f"✅ Job {job_id} reseteado a pending")
            except:
                pass

def token_required(f):
    """
    Decorador para verificar la validez del JWT en el encabezado 'Authorization'.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        
        # 1. Obtener el token del encabezado 'Authorization: Bearer <token>'
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            if auth_header.startswith('Bearer '):
                token = auth_header.split(' ')[1]

        if not token:
            return jsonify({
                "status": "error",
                "message": "Token de autenticación faltante o malformado."
            }), 401

        try:
            # 2. Decodificar (verificar la firma y la expiración)
            # Si la clave es incorrecta o está expirado, lanzará una excepción
            data = jwt.decode(token, JWT_SECRET_KEY, algorithms=["HS256"])
            # Se puede añadir la información del usuario al request si es necesario
            request.current_user = data.get('username') 
            
        except jwt.ExpiredSignatureError:
            return jsonify({
                "status": "error",
                "message": "Token expirado. Inicia sesión de nuevo."
            }), 401
        except jwt.InvalidSignatureError:
            return jsonify({
                "status": "error",
                "message": "Token inválido. Firma incorrecta."
            }), 401
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": f"Error de decodificación de token: {str(e)}"
            }), 401
            
        return f(*args, **kwargs)

    return decorated

# =================================================================
# NUEVO ENDPOINT HTTP (/qr-image)
# =================================================================

@app.route('/qr-image', methods=['GET'])
@token_required
def get_qr_image():
    """
    Llama al API de Baileys para obtener la cadena QR y la devuelve como imagen Base64.
    """
    BAILEYS_QR_URL = "http://localhost:3000/qr" 
    print("📡 Solicitando código QR al servidor Baileys...")
    try:
        # 1. Llamar al API de Node/Baileys para obtener la cadena QR
        response = requests.get(BAILEYS_QR_URL)
        print(f"📡 Respuesta del servidor Baileys: {response}")
        data = response.json()
        print(f"📡 Datos recibidos: {data}")
        # ===> CORRECCIÓN CLAVE: Usar notación de corchetes data['connected']
        if data.get('connected') == True: 
            return jsonify({"status": "connected", "message": "Ya está conectado"}), 200
        
        # El endpoint de Node.js devuelve el QR bajo la clave 'qr' si no está conectado.
        qr_string = data.get('qr')
        
        if not qr_string:
            return jsonify({"status": "waiting", "message": "Esperando generación de QR..."}), 200

        # ... (Resto del código para generar la imagen QR)
        # 2. Generar la imagen QR
        img = qrcode.make(qr_string)
        
        # 3. Guardar la imagen en un buffer de memoria
        buffer = BytesIO()
        img.save(buffer, format="PNG")
        
        # 4. Codificar a Base64
        qr_base64 = b64encode(buffer.getvalue()).decode('utf-8')
        
        # 5. Devolver la imagen en formato URI de datos
        return jsonify({
            "status": "qr_ready",
            "qr_data_uri": f"data:image/png;base64,{qr_base64}",
            "message": "Escanea este código"
        }), 200

    except Exception as e:
        # Aquí puedes ver los errores de conexión, JSON, etc.
        print(f"❌ Error al procesar QR-Image: {e}")
        return jsonify({
            "status": "error", 
            "message": "Error al generar QR", 
            "details": str(e)
        }), 500

@app.route('/jobs/recover', methods=['POST'])
@token_required
def recover_jobs():
    """Endpoint para recuperar jobs colgados manualmente"""
    try:
        recovered = recover_stuck_jobs()
        return jsonify({
            "status": "success",
            "message": f"Se recuperaron {recovered} jobs",
            "recovered": recovered
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
    
# =================================================================
# ENDPOINT HTTP (/send)
# =================================================================

@app.route('/send', methods=['POST'])
@token_required
def send_message_service():
    """
    Recibe un JSON con 'mensaje' y 'numeros' y lo envía al servidor Baileys.
    """
    try:
        data = request.get_json()
        
        # 1. Validación de datos de entrada
        message = data.get('mensaje')
        contactos = data.get('contactos') 
        
        if not message or not contactos:
            return jsonify({
                "status": "error",
                "message": "Faltan parámetros: 'mensaje' o 'contactos'",
                "details": data
            }), 400

        
        if not isinstance(contactos, list):
            return jsonify({
                "error": "Datos de entrada contacto no válidos",
                "required_format": {"mensaje": "string", "contacto": ["string1", "string2"]}
            }), 400
        
        # 2. Formateo y preparación del payload
        contacts = clean_and_validate_contacts(contactos)
        if not contacts:
            return jsonify({"error": "No hay contactos válidos"}), 400
        
        job_id = str(uuid4())

        # Guardar metadata
        redis_client.set(f"job:{job_id}", json.dumps({
            "message": message,
            "created_at": datetime.now().isoformat(),
            "status": "pending"
        }))
        
        # Guardar contactos
        for c in contacts:
            redis_client.rpush(f"job:{job_id}:contacts", json.dumps({
                "numero": c["numero"],
                "nombre": c.get("nombre", "Cliente"),
                "estado": 0
            }))


        # Agregar a la cola de manera segura
        safe_add_to_queue(job_id, time.time())
        
        print(f"✅ Job creado: {job_id} con {len(contacts)} contactos")

        # 3. Envío al servidor Baileys
        return jsonify({
            "status": "queued",
            "job_id": job_id,
            "total": len(contacts)
        }), 202

    except requests.exceptions.ConnectionError:
        return jsonify({
            "status": "error_connection",
            "message": f"No se pudo conectar al servidor Baileys en {BAILEYS_API_URL}"
        }), 503 # Service Unavailable
        
    except Exception as e:
        print(f"❌ Error interno del servicio: {e}")
        return jsonify({
            "status": "error_internal",
            "message": "Ocurrió un error inesperado en el servicio Python",
            "details": str(e)
        }), 500

def clean_and_validate_contacts(raw_contacts):
    """
    Espera una lista de objetos:
    [
        { "nombre": "Juan", "numero": "987 654 321" },
        { "nombre": "María", "numero": "+51 912-345-678" }
    ]
    """

    valid = []
    seen = set()

    for contact in raw_contacts:
        # Validar estructura básica
        if not isinstance(contact, dict):
            continue

        nombre = (contact.get("nombre") or "Cliente").strip()
        numero_raw = contact.get("numero")

        if not numero_raw:
            continue

        # Limpiar número: solo dígitos
        num = re.sub(r"\D", "", str(numero_raw))

        # Caso Perú:
        # - 9 dígitos empezando en 9
        # - o 11 dígitos empezando en 51
        if len(num) == 9 and num.startswith("9"):
            full = CODIGO_PAIS + num
        elif len(num) == 11 and num.startswith(CODIGO_PAIS):
            full = num
        else:
            continue  # número inválido

        # Evitar duplicados
        if full in seen:
            continue

        seen.add(full)

        # Guardar contacto limpio
        valid.append({
            "nombre": nombre,
            "numero": full
        })

    return valid

@app.route('/status/<job_id>', methods=['GET'])
@token_required
def job_status(job_id):
    job_data_raw = redis_client.get(f"job:{job_id}")

    if not job_data_raw:
        return jsonify({"error": "Job no encontrado"}), 404

    job_data = json.loads(job_data_raw)

    contacts = redis_client.lrange(f"job:{job_id}:contacts", 0, -1)

    total = len(contacts)
    enviados = 0
    errores = 0
    pendientes = 0

    for c in contacts:
        estado = json.loads(c)["estado"]
        if estado == 1:
            enviados += 1
        elif estado == 2:
            errores += 1
        else:
            pendientes += 1

    return jsonify({
        "job_id": job_id,
        "status": job_data.get("status", "pending"),
        "total": total,
        "sent": enviados,
        "errors": errores,
        "pending": pendientes
    }), 200



@app.post('/logout')
@token_required
def logout_session():
    """
    Reenvía la petición POST al servidor Baileys para cerrar la sesión.
    """
    BAILEYS_LOGOUT_URL = "http://localhost:3000/logout" 
    
    try:
        print("🚪 Solicitando cierre de sesión a Baileys...")
        # Reenvía la petición POST al servidor Baileys/Node.js
        # No necesitamos body, solo la solicitud de cierre
        response = requests.post(BAILEYS_LOGOUT_URL)
        
        # Devuelve la respuesta de Baileys directamente
        # Nota: El servidor Baileys debe devolver un JSON y código 200/202 si es exitoso
        return jsonify(response.json()), response.status_code
        
    except requests.exceptions.ConnectionError:
        print(f"❌ Error: No se pudo conectar al servidor Baileys en {BAILEYS_LOGOUT_URL}")
        return jsonify({
            "status": "error", 
            "message": "Servidor Baileys (Node.js) no disponible para logout"
        }), 503
    except Exception as e:
        print(f"❌ Error interno de Flask al procesar logout: {e}")
        return jsonify({
            "status": "error", 
            "message": "Error interno al procesar logout", 
            "details": str(e)
        }), 500

# =================================================================
# NUEVO ENDPOINT HTTP (/reset-session)
# =================================================================

@app.post('/reset-session')
@token_required
def reset_session():
    """
    Reenvía la petición POST al servidor Baileys para borrar la sesión y reiniciar.
    
    Nota: Se ASUME que Node.js tiene un endpoint en http://localhost:3000/restart
    que maneja el cierre, limpieza de archivos y reinicio de la conexión.
    """
    BAILEYS_RESET_URL = "http://localhost:3000/restart" 
    
    try:
        print("🚨 Solicitando REINICIO de sesión a Baileys...")
        # Reenvía la petición POST al servidor Baileys/Node.js
        response = requests.post(BAILEYS_RESET_URL)
        
        # Devuelve la respuesta de Baileys directamente
        return jsonify(response.json()), response.status_code
        
    except requests.exceptions.ConnectionError:
        print(f"❌ Error: No se pudo conectar al servidor Baileys en {BAILEYS_RESET_URL}. Verifica que Node.js esté corriendo.")
        return jsonify({
            "status": "error", 
            "message": "Servidor Baileys (Node.js) no disponible para reset."
        }), 503
    except Exception as e:
        print(f"❌ Error interno de Flask al procesar el reset: {e}")
        return jsonify({
            "status": "error", 
            "message": "Error interno al procesar reset de sesión.", 
            "details": str(e)
        }), 500    
    
@app.route('/login', methods=['POST'])
def login_user():
    """
    Verifica las credenciales fijas y genera un JWT si son correctas.
    """
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    # 1. Verificar las credenciales fijas
    if username == FIXED_USERNAME and password == FIXED_PASSWORD:
        
        # 2. Generar el payload del token (incluye el tiempo de expiración)
        expiration_time = datetime.now(timezone.utc) + timedelta(minutes=TOKEN_EXPIRATION_MINUTES)
        
        token_payload = {
            'username': username,
            'exp': expiration_time,
            'iat': datetime.now(timezone.utc)
        }
        
        # 3. Firmar el token usando la clave secreta
        token = jwt.encode(token_payload, JWT_SECRET_KEY, algorithm="HS256")
        
        return jsonify({
            "status": "success",
            "message": "Inicio de sesión exitoso.",
            "token": token
        }), 200
    else:
        # 4. Credenciales incorrectas
        return jsonify({
            "status": "error",
            "message": "Credenciales incorrectas."
        }), 401
    
@app.route('/jobs', methods=['GET'])
@token_required
def list_jobs():
    """
    Lista todos los lotes (jobs) con su información resumida.
    """
    try:
        # Verificar que jobs_queue existe
        if not redis_client.exists("jobs_queue"):
            return jsonify({
                "status": "error",
                "message": "Cola de jobs no encontrada"
            }), 500
        
        # Obtener todos los elementos de la cola
        all_queue_items = redis_client.zrange("jobs_queue", 0, -1)
        
        # Filtrar el placeholder (temp o __PLACEHOLDER__)
        # Excluir elementos que son placeholders
        excluded = ["temp", "__PLACEHOLDER__"]
        active_jobs = [job for job in all_queue_items if job not in excluded]
        
        print(f"📋 Jobs activos en cola: {active_jobs}")
        
        # Buscar todos los jobs (metadata)
        all_job_keys = redis_client.keys("job:*")
        all_jobs_set = set()
        
        for key in all_job_keys:
            if ':contacts' not in key:
                job_id = key.replace("job:", "")
                all_jobs_set.add(job_id)
        
        all_jobs = list(all_jobs_set)
        jobs_list = []
        
        for job_id in all_jobs:
            try:
                # Obtener metadata del job
                job_data_raw = redis_client.get(f"job:{job_id}")
                if not job_data_raw:
                    print(f"⚠️ Job {job_id} sin metadata")
                    continue
                
                job_data = json.loads(job_data_raw)
                
                # Obtener contactos
                contacts_key = f"job:{job_id}:contacts"
                contacts_type = redis_client.type(contacts_key)
                
                total = 0
                enviados = 0
                fallidos = 0
                pendientes = 0
                
                if contacts_type == 'list':
                    contacts = redis_client.lrange(contacts_key, 0, -1)
                    total = len(contacts)
                    
                    for c in contacts:
                        try:
                            contacto = json.loads(c)
                            estado = contacto.get("estado", 0)
                            if estado == 1:
                                enviados += 1
                            elif estado == 2:
                                fallidos += 1
                            else:
                                pendientes += 1
                        except json.JSONDecodeError:
                            pendientes += 1
                
                # Construir objeto del lote
                job_info = {
                    "id_lote": job_id,
                    "mensaje": job_data.get("message", "")[:100],
                    "cant_contactos": total,
                    "contactos_enviados": enviados,
                    "contactos_fallidos": fallidos,
                    "contactos_pendientes": pendientes,
                    "estado_lote": job_data.get("status", "unknown"),
                    "fecha_lote": job_data.get("created_at", ""),
                    "fecha_finalizacion": job_data.get("finished_at", ""),
                    "progreso": round((enviados + fallidos) / total * 100, 2) if total > 0 else 0,
                    "en_cola": job_id in active_jobs  # Indicar si está en cola
                }
                
                jobs_list.append(job_info)
                
            except Exception as e:
                print(f"❌ Error procesando job {job_id}: {e}")
                continue
        
        # Ordenar por fecha (más recientes primero)
        jobs_list.sort(key=lambda x: x["fecha_lote"], reverse=True)
        
        # Estadísticas generales
        stats = {
            "total_jobs": len(jobs_list),
            "pending": len([j for j in jobs_list if j["estado_lote"] == "pending"]),
            "processing": len([j for j in jobs_list if j["estado_lote"] == "processing"]),
            "completed": len([j for j in jobs_list if j["estado_lote"] == "completed"]),
            "en_cola": len(active_jobs)
        }
        
        return jsonify({
            "status": "success",
            "stats": stats,
            "jobs": jobs_list,
            "total": len(jobs_list)
        }), 200
        
    except Exception as e:
        print(f"❌ Error al listar jobs: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "status": "error",
            "message": "Error al obtener la lista de lotes",
            "details": str(e)
        }), 500


def initialize_redis_structure():
    """Inicializa la estructura de Redis automáticamente al arrancar"""
    print("🔧 Inicializando estructura de Redis...")
    
    # 1. Crear jobs_queue si no existe
    if not redis_client.exists("jobs_queue"):
        print("⚠️ jobs_queue no existe, creando...")
        # Crear con un placeholder que nunca se procesará
        redis_client.zadd("jobs_queue", {"__PLACEHOLDER__": 0})
        print("✅ jobs_queue creado con placeholder")
    
    # 2. Verificar tipo correcto
    queue_type = redis_client.type("jobs_queue")
    if queue_type != 'zset':
        print(f"⚠️ jobs_queue es {queue_type}, corrigiendo...")
        # Guardar datos existentes
        old_data = []
        if queue_type == 'list':
            old_data = redis_client.lrange("jobs_queue", 0, -1)
        elif queue_type == 'set':
            old_data = list(redis_client.smembers("jobs_queue"))
        
        # Recrear como ZSET
        redis_client.delete("jobs_queue")
        redis_client.zadd("jobs_queue", {"__PLACEHOLDER__": 0})
        
        # Restaurar datos reales (excluyendo placeholders viejos)
        for item in old_data:
            if item not in ["temp", "__PLACEHOLDER__"]:
                redis_client.zadd("jobs_queue", {item: time.time()})
        print(f"✅ jobs_queue convertido a ZSET con {redis_client.zcard('jobs_queue') - 1} jobs reales")
    
    # 3. Limpiar jobs huérfanos automáticamente
    print("🔍 Limpiando jobs huérfanos...")
    all_queue_items = redis_client.zrange("jobs_queue", 0, -1)
    cleaned = 0
    
    for job_id in all_queue_items:
        # Saltar placeholder
        if job_id == "__PLACEHOLDER__":
            continue
        
        # Verificar si el job existe
        if not redis_client.exists(f"job:{job_id}"):
            print(f"  🗑️ Eliminando job huérfano: {job_id}")
            redis_client.zrem("jobs_queue", job_id)
            cleaned += 1
        
        # Verificar si tiene contactos
        elif not redis_client.exists(f"job:{job_id}:contacts"):
            print(f"  🗑️ Eliminando job sin contactos: {job_id}")
            redis_client.zrem("jobs_queue", job_id)
            cleaned += 1
    
    if cleaned > 0:
        print(f"✅ Limpiados {cleaned} jobs huérfanos")
    else:
        print("✅ No se encontraron jobs huérfanos")
    
    # 4. Mostrar estado final
    total_in_queue = redis_client.zcard("jobs_queue")
    real_jobs = total_in_queue - 1  # Restar el placeholder
    print(f"📊 Estado final: {real_jobs} jobs reales en cola + 1 placeholder")
    
    return True


def safe_add_to_queue(job_id, score=None):
    """Agregar un job a la cola de manera segura"""
    if score is None:
        score = time.time()
    
    # Asegurar que la cola existe con placeholder
    if not redis_client.exists("jobs_queue"):
        redis_client.zadd("jobs_queue", {"__PLACEHOLDER__": 0})
    
    # Agregar el job real
    redis_client.zadd("jobs_queue", {job_id: score})
    print(f"✅ Job {job_id} agregado a la cola (score: {score})")
    
    return True

def safe_remove_from_queue(job_id):
    """Eliminar un job de la cola de manera segura (NUNCA elimina el placeholder)"""
    if job_id == "__PLACEHOLDER__":
        return False
    
    redis_client.zrem("jobs_queue", job_id)
    print(f"✅ Job {job_id} eliminado de la cola")
    return True

def safe_get_queue_jobs():
    """Obtener solo los jobs reales de la cola (excluyendo placeholder)"""
    if not redis_client.exists("jobs_queue"):
        return []
    
    all_items = redis_client.zrange("jobs_queue", 0, -1)
    # Excluir el placeholder
    excluded = ["temp", "__PLACEHOLDER__"]
    return [item for item in all_items if item not in excluded]

# =================================================================
# USO DEL SCRIPT
# =================================================================

if __name__ == "__main__":
    
    # 💡 PERSONALIZA ESTOS VALORES
    # Inicializar estructura de Redis
    initialize_redis_structure()
    message_to_send = "¡Hola! Esta es una prueba de envío masivo desde mi script Python Baileys.💡 🎉"
    Thread(target=scheduler_loop, daemon=True).start()
    # Lista de números de teléfono (pueden ser strings con o sin formato)
    # NOTA: Los números DEBEN existir en WhatsApp y tener el código de país
    # si decides no usar la variable CODIGO_PAIS de arriba.
    contact_numbers = [
        "922546925"  # Tercer número
    ]
    print("=========================================")
    print("🚀 SERVICIO DE ENVÍO WHATSAPP PYTHON INICIADO")
    print("📡 Escuchando en http://127.0.0.1:5000/send")
    print("=========================================")
    CORS(app)
    print("⏰ Iniciando scheduler para procesar la cola de mensajes...")
    
    # Usa debug=True solo para desarrollo.
    app.run(host='0.0.0.0', port=5000)