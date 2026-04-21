# 🏦 AntifraudeBancarioDEMO: Plataforma Educativa de Detección de Fraude en Tiempo Real

![Banner](https://capsule-render.vercel.app/api?type=waving&color=gradient&height=200&section=header&text=Demo%20Antifraude%20Bancario&fontSize=50&fontAlignY=38&desc=Detecci%C3%B3n%20en%20Tiempo%20Real%20basada%20en%20IA%20y%20Eventos&descAlignY=60&descAlign=62)

<div align="center">
  <img src="https://img.shields.io/badge/Status-Active-success.svg?style=for-the-badge&logo=github" />
  <img src="https://img.shields.io/badge/Docker-Enabled-blue.svg?style=for-the-badge&logo=docker" />
  <img src="https://img.shields.io/badge/Kafka-Streaming-orange.svg?style=for-the-badge&logo=apachekafka" />
  <img src="https://img.shields.io/badge/Python-Microservices-yellow.svg?style=for-the-badge&logo=python" />
</div>

<br/>

Este repositorio contiene un ecosistema de microservicios diseñado para ilustrar y emular el funcionamiento de un **sistema antifraude bancario moderno**. Demuestra en tiempo real cómo se orquestan componentes de procesamiento de transacciones, integración de datos y evaluación por IA (*Machine Learning*). 

Desarrollado para propósitos analíticos y educativos, este proyecto permite entender el ciclo de vida completo de un evento fraudulento.

---

## 🏗️ Arquitectura y Componentes del Sistema

El entorno está completamente dockerizado y simula la complejidad de una red distribuida de microservicios, comunicados asincrónicamente mediante flujos de eventos.

### 🌐 Microservicios Principales:
1. **`transaction-generator`**: Un simulador constante de estrés que genera y eyecta transacciones sintéticas hacia Kafka, algunas legítimas y otras fraudulentas de manera controlada.
2. **`fraud-scoring` (API REST)**: El motor analítico central. Escucha los eventos, consulta una caché ultrarrápida (Redis) y evalúa el riesgo devolviendo un dictamen y puntuación.
3. **`enrichment`**: Un componente pasivo que intercepta la transacción, la enriquece (por ejemplo, validando IPs o datos de la entidad bancaria) y reenvía los datos procesados.
4. **`case-management`**: Registra los datos analizados a largo plazo en Postgres y maneja las alertas de fraude que requieren revisión humana.
5. **`dashboard`**: Una interfaz visual sencilla para ver cómo los componentes actúan de manera conjunta.

### ⚙️ Infraestructura y Observabilidad:
- **Broker de Eventos:** Apache Kafka + Zookeeper (Auditoría visual a través de *Kafdrop*).
- **Almacenamiento:** PostgreSQL (Datos Persistentes) y Redis (Caché en Memoria).
- **Monitorización:** Prometheus (Métricas) y Grafana (Dashboards visuales de rendimiento).

---

## 🚀 Guía de Inicio Rápido (Cómo ejecutar)

Para desplegar este ecosistema completo en tu máquina local de forma automatizada, utilizamos un entorno de orquestación `docker-compose`.

### Prerrequisitos:
- Docker y Docker Compose instalados.
- Make (Opcional, pero recomendado para el uso de comandos simplificados).

### 1️⃣ Levantar el ecosistema (Start)
Si tienes `make` instalado, simplemente ejecuta:
```bash
make up
```
*(Si no usas make, emplea: `docker compose up -d --build`)*

### 2️⃣ Accesibilidad de Servicios
Una vez inicializado, tendrás todos los portales y APIs expuestos localmente:

| Servicio | Enlace de Acceso Local | Credenciales por defecto |
|----------|-------------------------|--------------------------|
| **Dashboard UI** | [http://localhost:3001](http://localhost:3001) | *Sin acceso restringido* |
| **API de Scoring** | [http://localhost:8001/docs](http://localhost:8001/docs) | *Acceso Swagger API* |
| **Monitoreo Kafdrop**| [http://localhost:9000](http://localhost:9000) | - |
| **Métricas Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin123` |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | - |

*(Para ver esta lista en consola, ejecuta `make urls`)*

---

## 🛠️ Comandos de Administración (`Makefile`)

El proyecto dispone de un `Makefile` con comandos rápidos para la gestión del clúster:

- `make logs`: Verifica los logs de todos los microservicios en tiempo real.
- `make ps`: Lista los contenedores en ejecución.
- `make generator-logs`: Sigue únicamente las transacciones creadas por el generador.
- `make scoring-logs`: Ver logs de las decisiones probabilísticas del motor de IA antifraude.
- `make clean`: Detiene todo y destruye los volúmenes, devolviendo el entorno a 0.

---

## 📚 Flujo de Datos Explicado (Educativo)

Cuando arranca el entorno, ocurre el siguiente proceso:
1. El **Transaction Generator** expulsa un objeto JSON por un puerto Kafka de entrada (`txn.raw`).
2. El servicio **Enrichment** intercepta `txn.raw`, añade metadatos geolocalizados/bancarios y lo desplaza a la cola `txn.enriched`.
3. El **Fraud Scoring** recoge el mensaje de `txn.enriched`, examina su historia contra Redis, y corre un modelo predictivo, lanzando a su vez el veredicto en `fraud.alerts` o `txn.scored`.
4. Finalmente, el **Case Management** ingiere `fraud.alerts`, graba de forma inmutable el evento en PostgreSQL y actualiza el Dashboard para que el "equipo de analistas del banco" pueda verlo.

Esto simula un ambiente altamente desacoplado, resiliente y de latencia ínfima.

---

> _**Aviso:** Todos los datos generados son estrictamente ficticios y con intenciones de pruebas en I+D._

<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&height=100&section=footer" />
</p>
