import redis
import json
import time
from datetime import datetime


redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def fix_redis_complete():
    print("🔧 REPARACIÓN COMPLETA DE REDIS")
    print("=" * 50)
    
    # 1. Verificar y crear jobs_queue
    if not redis_client.exists("jobs_queue"):
        print("⚠️ jobs_queue no existe, creando...")
        # Crear ZSET vacío - usar un comando que funcione
        # Opción 1: Usar zadd con un elemento temporal y luego eliminarlo
        redis_client.zadd("jobs_queue", {"__temp__": 0})
        redis_client.zrem("jobs_queue", "__temp__")
        print("✅ jobs_queue creado como ZSET vacío")
    else:
        queue_type = redis_client.type("jobs_queue")
        if queue_type != 'zset':
            print(f"⚠️ jobs_queue es {queue_type}, convirtiendo a ZSET...")
            # Guardar datos existentes
            old_data = []
            if queue_type == 'list':
                old_data = redis_client.lrange("jobs_queue", 0, -1)
            elif queue_type == 'set':
                old_data = list(redis_client.smembers("jobs_queue"))
            
            # Recrear como ZSET
            redis_client.delete("jobs_queue")
            # Crear ZSET vacío
            redis_client.zadd("jobs_queue", {"__temp__": 0})
            redis_client.zrem("jobs_queue", "__temp__")
            
            # Restaurar datos
            for item in old_data:
                redis_client.zadd("jobs_queue", {item: time.time()})
            print(f"✅ Convertido a ZSET con {len(old_data)} items")
    
    # 2. Buscar todos los jobs
    job_keys = redis_client.keys("job:*")
    jobs_found = []
    
    for key in job_keys:
        if ':contacts' not in key:
            job_id = key.replace("job:", "")
            jobs_found.append(job_id)
    
    print(f"\n📋 Jobs encontrados: {len(jobs_found)}")
    for job_id in jobs_found:
        # Obtener metadata
        job_data = redis_client.get(f"job:{job_id}")
        if job_data:
            try:
                data = json.loads(job_data)
                status = data.get('status', 'unknown')
                created_at = data.get('created_at', '')
                print(f"  - {job_id}: estado={status}, creado={created_at}")
            except:
                print(f"  - {job_id}: error al parsear")
    
    # 3. Verificar qué jobs están en la cola
    jobs_in_queue = redis_client.zrange("jobs_queue", 0, -1)
    print(f"\n📋 Jobs en cola: {len(jobs_in_queue)}")
    
    # 4. Agregar jobs pendientes o en proceso a la cola
    added_count = 0
    for job_id in jobs_found:
        if job_id not in jobs_in_queue:
            job_data = redis_client.get(f"job:{job_id}")
            if job_data:
                try:
                    data = json.loads(job_data)
                    status = data.get('status')
                    
                    # Solo agregar si está pendiente o en proceso
                    if status in ['pending', 'processing']:
                        # Usar timestamp de creación como score
                        created_at = data.get('created_at')
                        if created_at:
                            try:
                                # Convertir ISO string a timestamp
                                dt = datetime.fromisoformat(created_at)
                                score = dt.timestamp()
                            except:
                                score = time.time()
                        else:
                            score = time.time()
                        
                        redis_client.zadd("jobs_queue", {job_id: score})
                        print(f"  ✅ Agregado a cola: {job_id} (estado: {status})")
                        added_count += 1
                    else:
                        print(f"  ⏭️ Job completado, no agregado: {job_id} (estado: {status})")
                except Exception as e:
                    print(f"  ❌ Error procesando {job_id}: {e}")
    
    if added_count == 0:
        print("\n⚠️ No se agregaron jobs a la cola porque todos están completados")
        print("   Para probar, crea un nuevo job con estado 'pending'")
    
    # 5. Mostrar estadísticas finales
    print("\n" + "=" * 50)
    print("📊 ESTADÍSTICAS FINALES:")
    
    queue_type = redis_client.type("jobs_queue")
    queue_size = redis_client.zcard("jobs_queue") if queue_type == 'zset' else 0
    print(f"✅ jobs_queue: {queue_type} con {queue_size} elementos")
    print(f"✅ Total jobs: {len(jobs_found)}")
    print(f"✅ Total listas contactos: {len(redis_client.keys('job:*:contacts'))}")
    print(f"✅ Total logs: {redis_client.llen('logs')}")
    
    # 6. Si hay jobs en cola, mostrarlos
    if queue_size > 0:
        print("\n📋 Jobs en cola (ordenados por antigüedad):")
        jobs = redis_client.zrange("jobs_queue", 0, -1, withscores=True)
        for job_id, score in jobs:
            job_data = redis_client.get(f"job:{job_id}")
            if job_data:
                data = json.loads(job_data)
                status = data.get('status')
                created = data.get('created_at', '')[:19]
                print(f"  - {job_id[:8]}... | score={score:.0f} | estado={status} | creado={created}")
    
    print("\n✅ Reparación completada")

def create_test_job():
    """Crear un job de prueba para verificar que funciona"""
    from uuid import uuid4
    
    print("\n" + "=" * 50)
    print("📝 CREANDO JOB DE PRUEBA")
    print("=" * 50)
    
    job_id = str(uuid4())
    
    # Datos de prueba
    job_data = {
        "message": "Este es un mensaje de prueba",
        "created_at": datetime.now().isoformat(),
        "status": "pending"
    }
    
    # Contactos de prueba
    test_contacts = [
        {"numero": "51999999999", "nombre": "Prueba 1", "estado": 0},
        {"numero": "51999999998", "nombre": "Prueba 2", "estado": 0}
    ]
    
    # Guardar en Redis
    redis_client.set(f"job:{job_id}", json.dumps(job_data))
    
    contacts_key = f"job:{job_id}:contacts"
    redis_client.delete(contacts_key)
    for contact in test_contacts:
        redis_client.rpush(contacts_key, json.dumps(contact))
    
    # Agregar a la cola
    score = time.time()
    redis_client.zadd("jobs_queue", {job_id: score})
    
    print(f"✅ Job de prueba creado:")
    print(f"   ID: {job_id}")
    print(f"   Estado: pending")
    print(f"   Contactos: {len(test_contacts)}")
    print(f"   Score: {score}")
    
    return job_id

if __name__ == "__main__":
    # Ejecutar reparación
    fix_redis_complete()
    
    # Preguntar si quiere crear un job de prueba
    print("\n" + "=" * 50)
    respuesta = input("¿Deseas crear un job de prueba? (s/n): ")
    if respuesta.lower() == 's':
        create_test_job()
        
        # Verificar resultado
        print("\n📋 Contenido actual de jobs_queue:")
        jobs = redis_client.zrange("jobs_queue", 0, -1, withscores=True)
        for job_id, score in jobs:
            job_data = redis_client.get(f"job:{job_id}")
            if job_data:
                data = json.loads(job_data)
                print(f"  - {job_id[:8]}... | estado: {data.get('status')} | score: {score:.0f}")