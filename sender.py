from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import json
import qrcode
from io import BytesIO 
from base64 import b64encode
import jwt
import datetime
app = Flask(__name__)
# =================================================================
# CONFIGURACIÓN
# =================================================================

# La URL de tu servidor Baileys (asegúrate de que esté corriendo)
BAILEYS_API_URL = "http://localhost:3000/send-bulk"

# El código de país que usarás.
# EJEMPLO: Si todos tus números son de Perú, usarías '51'
# Si los números YA incluyen el código de país, déjalo vacío o manéjalo con lógica.
CODIGO_PAIS = "51" 
JWT_SECRET_KEY = "xJvOFRengB9iMGoCtTH0yDV6wL45ZuWN"

FIXED_USERNAME = "admin"
FIXED_PASSWORD = "certifact123"
TOKEN_EXPIRATION_MINUTES = 30

from functools import wraps
# =================================================================
# FUNCIÓN DE ENVÍO
# =================================================================
def format_numbers(raw_numbers: list):
    """Formatea los números para cumplir con el requisito de Baileys (código de país)."""
    
    formatted_numbers = []
    for num in raw_numbers:
        cleaned_num = "".join(filter(str.isdigit, num))
        
        # Lógica de prefijo (solo si el código de país está configurado)
        if CODIGO_PAIS and not cleaned_num.startswith(CODIGO_PAIS):
            formatted_num = CODIGO_PAIS + cleaned_num
        else:
            formatted_num = cleaned_num
            
        formatted_numbers.append(formatted_num)
        
    return formatted_numbers

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

        # Almacenar resultados del envío
        results = []
        successful_count = 0

        
        if not isinstance(contactos, list):
            return jsonify({
                "error": "Datos de entrada contacto no válidos",
                "required_format": {"mensaje": "string", "contacto": ["string1", "string2"]}
            }), 400

        # 2. Formateo y preparación del payload
        formatted_contactos = format_contactos(contactos)
        if not formatted_contactos:
             return jsonify({
                "status": "error",
                "message": "Ningún contacto fue válido después de la limpieza y formateo (revisa que tengan 9 dígitos)."
            }), 400
        print(f"📢 Se enviará mensajes a {len(formatted_contactos)} contactos válidos.")
        payload = {
            "message": message,
            "contacts": formatted_contactos,
            "delay": 1000 # Retraso entre mensajes en milisegundos (ajusta si es necesario)
        }

        print(f"📢 Enviando {len(formatted_contactos)} mensajes al API de Baileys...")

        # 3. Envío al servidor Baileys
        response = requests.post(
            BAILEYS_API_URL, 
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        
        # 4. Devolver la respuesta de Baileys
        baileys_response = response.json()
        
        if response.status_code == 200:
            return jsonify({
                "status": "success",
                "message": "Solicitud de envío masivo enviada al servidor Baileys.",
                "details": baileys_response
            }), 200
        else:
            # Si Baileys devuelve un error (ej. WhatsApp no conectado)
            return jsonify({
                "status": "error_baileys",
                "message": "El servidor Baileys respondió con un error",
                "details": baileys_response
            }), response.status_code

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

def format_contactos(contactos):
    """
    Valida y formatea la lista de contactos recibida del frontend.
    
    Cada contacto es un objeto: { "numero": "...", "nombre": "..." }
    
    Reglas de validación:
    1. El número debe tener exactamente 9 dígitos. (Asume formato local sin código de país).
    2. Si el número no cumple, la fila se omite.
    3. Si el nombre es vacío o None, se reemplaza por "Cliente".

    Retorna una lista de contactos limpios y listos para ser enviados.
    Ejemplo: [ {"numero": "51987654321", "nombre": "Juan Pérez"}, ... ]
    """
    formatted_list = []
    
    for contacto in contactos:
        raw_numero = str(contacto.get('numero', '')).strip()
        raw_nombre = str(contacto.get('nombre', '')).strip()
        
        # 1. Limpiar el número: quitar espacios, guiones y cualquier carácter no dígito
        cleaned_numero = ''.join(filter(str.isdigit, raw_numero))
        
        # 2. Validación de longitud (asume 9 dígitos locales sin código de país)
        if len(cleaned_numero) != 9:
            # Omitir fila si no tiene 9 dígitos
            print(f"⚠️ Número omitido: '{raw_numero}' no tiene 9 dígitos.")
            continue
            
        # 3. Formateo final del número: añadir el código de país (Ej: 51 para Perú)
        # NOTA IMPORTANTE: Baileys requiere el código de país. 
        # Si todos tus contactos son de Perú, usa '51'. Ajusta este valor si es necesario.
        numero_con_codigo_pais = f"51{cleaned_numero}"

        # 4. Validar y reemplazar el nombre
        final_nombre = raw_nombre if raw_nombre else "Cliente"
        
        # 5. Agregar el contacto limpio a la lista
        formatted_list.append({
            "numero": numero_con_codigo_pais,
            "nombre": final_nombre
        })
        
    return formatted_list

def send_bulk_message(message: str, raw_numbers: list):
    """
    Consume el endpoint /send-bulk del servidor Baileys.
    Formatea los números agregando el código de país si no lo tienen.
    """
    
    # 1. Formateo de números (Asegurarse que todos tengan el código de país)
    # Baileys necesita el código de país al inicio (Ej. 51987654321)
    formatted_numbers = []
    for num in raw_numbers:
        # Limpia el número dejando solo dígitos
        cleaned_num = "".join(filter(str.isdigit, num))
        
        # Lógica simple: Si el número no empieza con el código de país, lo agrega
        if not cleaned_num.startswith(CODIGO_PAIS) and len(CODIGO_PAIS) > 0:
            formatted_num = CODIGO_PAIS + cleaned_num
        else:
            formatted_num = cleaned_num
            
        formatted_numbers.append(formatted_num)

    # 2. Creación del payload (JSON) para la API
    payload = {
        "message": message,
        "numbers": formatted_numbers,
        # Opcional: Establece un retraso en milisegundos entre cada mensaje
        "delay": 2000 
    }

    print("📢 Intentando enviar mensajes...")
    print(f"Número de destinatarios: {len(formatted_numbers)}")
    print(f"Mensaje a enviar: {message[:30]}...")
    
    # 3. Envío de la solicitud HTTP
    try:
        response = requests.post(
            BAILEYS_API_URL, 
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        
        # 4. Manejo de la respuesta
        if response.status_code == 200:
            print("\n✅ Solicitud enviada exitosamente al servidor.")
            print(json.dumps(response.json(), indent=4))
        else:
            print(f"\n❌ Error al comunicarse con el servidor (HTTP {response.status_code}):")
            print(response.text)
            
    except requests.exceptions.ConnectionError:
        print(f"\n❌ Error de conexión: Asegúrate de que el servidor Baileys esté corriendo en {BAILEYS_API_URL.split('/send-bulk')[0]}")
    except Exception as e:
        print(f"\n❌ Ocurrió un error inesperado: {e}")

@app.post('/logout')
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
        expiration_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=TOKEN_EXPIRATION_MINUTES)
        
        token_payload = {
            'username': username,
            'exp': expiration_time,
            'iat': datetime.datetime.now(datetime.timezone.utc)
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
    app.run(port=5000, debug=True)