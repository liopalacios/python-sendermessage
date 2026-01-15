from flask import Flask, request, jsonify
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
JOBS_LOCK = Lock()

from functools import wraps
# =================================================================
# FUNCIÓN DE ENVÍO
# =================================================================


def send_worker(job_id, message, contacts):
    errors_in_row = 0
    time.sleep(random.randint(5, 15)) # Espera inicial
    for idx, contact in enumerate(contacts):

        with JOBS_LOCK:
            if JOBS[job_id]["status"] != "running":
                return

        try:
            final_message = message + random.choice(["", " ", " 👍", " 💡", " 💻"])
            r = requests.post(
                BAILEYS_API_URL,
                json={"contacto": contact, "message": final_message},
                timeout=10
            )

            if r.status_code == 200:
                with JOBS_LOCK:
                    JOBS[job_id]["sent"] += 1
                errors_in_row = 0
            else:
                raise Exception(f"HTTP {r.status_code}")

        except Exception:
            with JOBS_LOCK:
                JOBS[job_id]["errors"] += 1
            errors_in_row += 1

            if errors_in_row >= 3:
                with JOBS_LOCK:
                    JOBS[job_id]["status"] = "stopped"
                    JOBS[job_id]["finished_at"] = datetime.utcnow().isoformat()
                return

        # Delay humano
        time.sleep(random.randint(3, 8))

        # Pausa larga cada 5 mensajes
        if (idx + 1) % 5 == 0:
            time.sleep(random.randint(30, 60))

    with JOBS_LOCK:
        JOBS[job_id]["status"] = "completed"
        JOBS[job_id]["finished_at"] = datetime.utcnow().isoformat()

    time.sleep(300)  # 5 minutos
    with JOBS_LOCK:
        JOBS.pop(job_id, None)


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

        with JOBS_LOCK:
            JOBS[job_id] = {
                "total": len(contacts),
                "sent": 0,
                "errors": 0,
                "status": "running",
                "started_at": datetime.utcnow().isoformat(),
                "finished_at": None
            }
        Thread(
            target=send_worker,
            args=(job_id, message, contacts),
            daemon=True
        ).start()
        print(f"📢 Enviando {len(contacts)} mensajes al API de Baileys...")

        # 3. Envío al servidor Baileys
        return jsonify({
            "status": "accepted",
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
    with JOBS_LOCK:
        job = JOBS.get(job_id)

    if not job:
        return jsonify({"error": "Job no encontrado"}), 404

    return jsonify(job), 200



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
    

# =================================================================
# USO DEL SCRIPT
# =================================================================

if __name__ == "__main__":
    
    # 💡 PERSONALIZA ESTOS VALORES
    
    message_to_send = "¡Hola! Esta es una prueba de envío masivo desde mi script Python Baileys.💡 🎉"
    
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
    # Usa debug=True solo para desarrollo.
    app.run(host='0.0.0.0', port=5000)