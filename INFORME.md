# TP Coordinación

---

## 1. Coordinación entre Instancias (Sum y Aggregation)

### Detección y Difusión de EOF en Sum

Al ser `input_queue` una cola compartida, el mensaje de EOF enviado por el Gateway para un cliente es consumido por una única réplica de Sum. Para coordinar a todas las instancias:

- Se utiliza un exchange de tipo Fanout.
- Cada réplica de Sum ejecuta un thread de control con una cola exclusiva vinculada a este exchange.
- La réplica que recibe el EOF original desde `input_queue` lo retransmite al Fanout, logrando que todas las réplicas de Sum se notifiquen de la finalización del cliente.
- Se utiliza un `threading.Event` para garantizar que el thread de control esté activo y suscrito antes de que el hilo principal consuma datos de `input_queue`, y un `threading.Lock` para el acceso concurrente al diccionario en memoria.

### Particionado y Sincronización Sum → Aggregation

Para evitar redundancia y saturación de red por *broadcast*:

- Cada réplica de Sum particiona sus totales acumulados calculando el destino mediante un hash: `zlib.crc32(fruit.encode('utf-8')) % AGGREGATION_AMOUNT`.
- Cada fruta se envía al exchange directo con routing key `f"{AGGREGATION_PREFIX}_{agg_index}"`. De este modo, cada instancia de Aggregation procesa un subconjunto disjunto de frutas.
- Al vaciar sus datos, cada Sum envía un EOF a cada una de las instancias de Aggregation.
- Cada Aggregator contabiliza los EOFs recibidos por cliente. Únicamente cuando alcanza `SUM_AMOUNT` EOFs calcula su Top 3 local y lo despacha a `join_queue`.
- El nodo Join espera los tops parciales de las instancias de Aggregation, consolida la lista final usando la comparación de la clase `FruitItem` y publica el resultado a `results_queue`.

---

## 2. Escalabilidad

### Clientes

- Cada uno es identificado con un UUID generado en el Gateway, el cual se identifica en el encabezado de cada mensaje.
- Los mensajes de distintos clientes se intercalan en las colas sin interferencia. Cada worker almacena el estado en memoria separado por cliente.
- Apenas un worker finaliza el flujo de un cliente, ejecuta `.pop(client_id)`, asegurando que el uso de memoria dependa únicamente de las consultas activas.

### Grandes Volúmenes de Datos

- La cola de entrada es atendida concurrentemente por las réplicas de Sum, balanceando la carga de lectura.
- Si un cliente envía millones de registros, cada réplica de Sum emitirá hacia la red únicamente un mensaje por cada fruta distinta, reduciendo drásticamente el tráfico hacia etapas posteriores.
- La particion de las frutas con el hash las divide entre los Aggregations, reduciendo el volumen a ordenar en cada uno.

### Cantidad de Controles

Se pueden añadir réplicas de Sum para mayor capacidad de sumatorias, o réplicas de Aggregation para procesar mayores catálogos de frutas.
Además, al derivarse colas y exchanges de variables de entorno sin nombres hardcodeados, el sistema se adapta a cualquier topología.

---

## 3. Graceful Shutdown

Los nodos Sum, Aggregation y Join capturan `SIGTERM`. Dado que Pika no es *thread-safe*, el middleware expone `stop_consuming_threadsafe()` que delega en `connection.add_callback_threadsafe` para detener el loop de forma segura.
Se liberan canales y conexiones y se espera el cierre del hilo secundario en Sum.
