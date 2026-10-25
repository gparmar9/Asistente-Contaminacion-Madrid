"""Levanta servicios del proyecto en local, en un solo terminal. Lo usa el Makefile.

    python scripts/local.py db api-usuario web       # lo que hace `make frontend`
    python scripts/local.py db phoenix rag agente api-usuario web   # `make dev`

Cada servicio corre como proceso hijo con su salida prefijada ([api-usuario], [web]...).
Se arrancan en orden y cada uno espera a que responda aquel del que depende. Ctrl+C, o
que se caiga cualquiera, para todos. `db` y `phoenix` son la excepción: son contenedores
de docker-compose, quedan levantados al salir y se paran con `make stop`.

Solo usa la librería estándar, para funcionar con cualquier Python 3.10+ del equipo.
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

RAIZ = Path(__file__).resolve().parent.parent
WINDOWS = os.name == "nt"
ESPERA_MAX_S = 240  # el RAG carga el modelo de embeddings: ~1 min en frío
URL_PHOENIX = "http://localhost:6006"

COLORES = ["36", "35", "33", "32", "34", "91"]
RESET = "\033[0m"


def python_de_venv(carpeta: Path) -> Path:
    return carpeta / ("Scripts/python.exe" if WINDOWS else "bin/python")


@dataclass
class Servicio:
    nombre: str
    puerto: int
    salud: str  # ruta que responde 200 cuando está listo
    cwd: Path
    comando: list[str]
    depende_de: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    requisitos: list[tuple[Path, str]] = field(default_factory=list)  # (fichero, cómo crearlo)

    @property
    def url_salud(self) -> str:
        return f"http://localhost:{self.puerto}{self.salud}"


def definir_servicios(pedidos: set[str]) -> dict[str, Servicio]:
    # Los servicios de la raíz (API, RAG) usan el Python con el que se lanza este script:
    # el venv del proyecto (el Makefile lo elige, se cambia con `make PY=...`).
    py = sys.executable
    py_agente = str(python_de_venv(RAIZ / "Agente/.venv"))

    # ApiUsuario habla con el agente que esté levantado en esta sesión. Si no hay ninguno,
    # se respeta lo que diga ApiUsuario/.env (vacío = /chat responde un stub).
    env_api: dict[str, str] = {}
    if url_db := url_base_de_datos(pedidos):
        env_api["DATABASE_URL"] = url_db
    depende_api: list[str] = []
    if "agente" in pedidos:
        env_api["AGENTE_URL"] = "http://localhost:8200"
        depende_api.append("agente")

    # Lo que el agente necesita saber de la sesión; el resto sale de Agente/.env.
    env_agente: dict[str, str] = {}
    if "rag" in pedidos:
        env_agente["RAG_URL"] = "http://localhost:8010"
    if "phoenix" in pedidos:
        env_agente["PHOENIX_ENDPOINT"] = f"{URL_PHOENIX}/v1/traces"
    depende_rag = ["rag"] if "rag" in pedidos else []
    npm = shutil.which("npm") or "npm"

    servicios = [
        Servicio(
            "rag", 8010, "/salud", RAIZ,
            [py, "-m", "rag.api"],
            requisitos=[(RAIZ / "data/chroma", "python -m rag.indexar (ver README)")],
        ),
        Servicio(
            "agente", 8200, "/salud", RAIZ / "Agente",
            [py_agente, "-m", "uvicorn", "agente.main:app", "--app-dir", "src",
             "--port", "8200", "--env-file", ".env", "--reload"],
            depende_de=depende_rag, env=env_agente,
            requisitos=[(Path(py_agente), "make install-agente"),
                        (RAIZ / "Agente/.env", "copiar Agente/.env.example a Agente/.env y rellenarlo")],
        ),
        Servicio(
            "api-usuario", 8000, "/health", RAIZ / "ApiUsuario",
            [py, "-m", "uvicorn", "api_usuario.main:app", "--app-dir", "src",
             "--port", "8000", "--env-file", ".env", "--reload"],
            depende_de=depende_api, env=env_api,
            requisitos=[(RAIZ / "ApiUsuario/.env", "copiar ApiUsuario/.env.example a ApiUsuario/.env")],
        ),
        Servicio(
            "web", 5173, "/", RAIZ / "Web",
            [npm, "run", "dev", "--", "--strictPort"],
            depende_de=["api-usuario"] if "api-usuario" in pedidos else [],
            requisitos=[(RAIZ / "Web/node_modules", "make install")],
        ),
    ]
    return {s.nombre: s for s in servicios}


# --- utilidades -------------------------------------------------------------------------

def aviso(texto: str) -> None:
    print(f"\033[1m>> {texto}{RESET}", flush=True)


def fallo(texto: str) -> None:
    print(f"\033[91mERROR: {texto}{RESET}", file=sys.stderr, flush=True)
    sys.exit(1)


def responde(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            return r.status < 500
    except Exception:
        return False


def puerto_ocupado(puerto: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", puerto)) == 0


def leer_env(fichero: Path, clave: str) -> str:
    """Valor de una clave en un .env, sin cargar el resto (ni mostrarlo)."""
    if not fichero.exists():
        return ""
    for linea in fichero.read_text(encoding="utf-8").splitlines():
        if linea.strip().startswith(f"{clave}="):
            return linea.split("=", 1)[1].strip().strip("'\"")
    return ""


# --- base de datos ----------------------------------------------------------------------

def url_base_de_datos(pedidos: set[str]) -> str:
    """Base de datos de ApiUsuario. Manda DATABASE_URL del terminal (p. ej. la de RDS); si
    no, con `db` se usa la del .env de la raíz, el mismo del que docker-compose saca las
    credenciales del contenedor. Vacía = lo que diga ApiUsuario/.env."""
    if url := os.environ.get("DATABASE_URL"):
        return url
    return leer_env(RAIZ / ".env", "DATABASE_URL") if "db" in pedidos else ""


def levantar_db() -> None:
    url = url_base_de_datos({"db"})
    if not url:
        fallo("falta DATABASE_URL en el .env de la raíz (copiar .env.example a .env)")
    host = urlparse(url).hostname or ""
    if host and host not in ("localhost", "127.0.0.1"):
        aviso(f"DATABASE_URL apunta a {host}: uso esa base de datos, no levanto Docker")
        return
    if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        fallo("Docker no responde. Abre Docker Desktop, espera a que arranque y repite.")
    aviso("Levantando PostgreSQL (docker compose up -d --wait db)")
    if subprocess.run(["docker", "compose", "up", "-d", "--wait", "db"], cwd=RAIZ).returncode != 0:
        fallo("no se pudo levantar la base de datos (ver el mensaje de docker de arriba)")


def levantar_phoenix() -> None:
    """Phoenix (trazas del agente) en Docker. Sin él las trazas siguen yendo al JSONL."""
    if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        fallo("Docker no responde. Abre Docker Desktop, espera a que arranque y repite.")
    aviso("Levantando Phoenix (docker compose --profile observabilidad up -d phoenix)")
    orden = ["docker", "compose", "--profile", "observabilidad", "up", "-d", "phoenix"]
    # Sin prefijo: aquí no hay nginx delante y la interfaz se abre directamente en :6006.
    env = {**os.environ, "PHOENIX_HOST_ROOT_PATH": ""}
    if subprocess.run(orden, cwd=RAIZ, env=env).returncode != 0:
        fallo("no se pudo levantar Phoenix (ver el mensaje de docker de arriba)")
    limite = time.monotonic() + 60
    while not responde(URL_PHOENIX):
        if time.monotonic() > limite:
            fallo(f"Phoenix no responde en {URL_PHOENIX} tras 60 s")
        time.sleep(1)


def comprobar_aws() -> None:
    """Con Bedrock, las claves temporales caducan: mejor saberlo ahora que en la primera pregunta.

    Replica lo que hará el agente: si AWS_PROFILE está definido (en el terminal o en
    Agente/.env) usa ese perfil; si no, las variables AWS_* del terminal.
    """
    env_agente = RAIZ / "Agente/.env"

    def valor(clave: str) -> str:
        return os.environ.get(clave) or leer_env(env_agente, clave)

    if valor("LLM_PROVEEDOR") != "bedrock":
        return
    if not shutil.which("aws"):
        aviso("AVISO: AWS CLI no está instalada, no puedo comprobar las credenciales de Bedrock")
        return
    perfil = valor("AWS_PROFILE")
    orden = ["aws", "sts", "get-caller-identity"] + (["--profile", perfil] if perfil else [])
    if subprocess.run(orden, capture_output=True).returncode != 0:
        origen = f"el perfil '{perfil}'" if perfil else "las variables AWS_* del terminal"
        fallo(
            f"las credenciales de AWS ({origen}) no valen: ¿caducadas? Copia unas nuevas del "
            "portal del máster y vuelve a lanzarlo desde esta misma terminal"
        )


# --- procesos ---------------------------------------------------------------------------

class Supervisor:
    def __init__(self, con_contenedores: bool) -> None:
        self.con_contenedores = con_contenedores
        self.procesos: dict[str, subprocess.Popen] = {}
        self.parando = False

    def arrancar(self, s: Servicio, color: str) -> None:
        env = {**os.environ, **s.env, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
        extra = (
            {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if WINDOWS
            else {"start_new_session": True}
        )
        p = subprocess.Popen(
            s.comando, cwd=s.cwd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
            **extra,
        )
        self.procesos[s.nombre] = p
        prefijo = f"\033[{color}m[{s.nombre}]{RESET} "
        threading.Thread(target=self._volcar, args=(p, prefijo), daemon=True).start()

    @staticmethod
    def _volcar(p: subprocess.Popen, prefijo: str) -> None:
        assert p.stdout is not None
        for linea in iter(p.stdout.readline, b""):
            texto = linea.decode("utf-8", errors="replace").rstrip()
            print(prefijo + texto, flush=True)

    def caido(self) -> str | None:
        for nombre, p in self.procesos.items():
            if p.poll() is not None:
                return nombre
        return None

    def esperar(self, s: Servicio) -> None:
        limite = time.monotonic() + ESPERA_MAX_S
        while not responde(s.url_salud):
            if (nombre := self.caido()) is not None:
                raise RuntimeError(f"{nombre} se ha parado al arrancar (ver su salida arriba)")
            if time.monotonic() > limite:
                raise RuntimeError(f"{s.nombre} no responde en {s.url_salud} tras {ESPERA_MAX_S} s")
            time.sleep(1)

    def parar_todo(self) -> None:
        if self.parando:
            return
        self.parando = True
        for p in reversed(list(self.procesos.values())):
            if p.poll() is None:
                # uvicorn --reload y npm lanzan hijos: hay que matar el árbol entero.
                if WINDOWS:
                    subprocess.run(["taskkill", "/T", "/F", "/PID", str(p.pid)], capture_output=True)
                else:
                    try:
                        os.killpg(p.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
        for p in self.procesos.values():
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        aviso("Servicios parados" + (" (los contenedores siguen levantados: make stop)" if self.con_contenedores else ""))


def comprobar(servicios: list[Servicio]) -> None:
    errores = []
    for s in servicios:
        for fichero, como in s.requisitos:
            if not fichero.exists():
                errores.append(f"{s.nombre}: falta {fichero.relative_to(RAIZ)} -> {como}")
        if puerto_ocupado(s.puerto):
            errores.append(f"{s.nombre}: el puerto {s.puerto} ya está ocupado "
                           "(¿otra sesión de make abierta? ciérrala con Ctrl+C)")
    if errores:
        fallo("no se puede arrancar:\n  " + "\n  ".join(errores))


def preguntar() -> None:
    """Pregunta al chat por ApiUsuario (/chat/stream) y muestra el SSE como texto legible.

    Los datos llegan por variables de entorno (las pone make): P (pregunta), SESION (para
    seguir una conversación) y CRUDO=1 (el SSE tal cual). Por entorno y no por argumentos
    para que las tildes y los signos ¿? pasen igual por cmd.exe, PowerShell y bash.
    """
    import json

    pregunta = os.environ.get("P", "").strip()
    if not pregunta:
        fallo('uso: make preguntar P="¿Qué efectos tiene el NO2 en la salud?" [SESION=id] [CRUDO=1]')
    url_api = "http://localhost:8000"
    if not responde(f"{url_api}/health"):
        fallo("ApiUsuario no responde en el puerto 8000: levanta antes make dev en otra terminal")

    cuerpo = {"pregunta": pregunta}
    if sesion := os.environ.get("SESION", "").strip():
        cuerpo["session_id"] = sesion
    peticion = urllib.request.Request(
        f"{url_api}/chat/stream", data=json.dumps(cuerpo).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    crudo = os.environ.get("CRUDO", "") not in ("", "0")

    # Fases entre corchetes (solo cuando cambian: el heartbeat las repite), tokens seguidos,
    # y session_id y traza al final.
    evento, ultima_fase = None, None
    with urllib.request.urlopen(peticion, timeout=180) as r:
        for bruta in r:
            linea = bruta.decode("utf-8", errors="replace").rstrip("\r\n")
            if crudo:
                print(linea, flush=True)
                continue
            if linea.startswith("event: "):
                evento = linea[7:]
                continue
            if not linea.startswith("data: "):
                if linea and not linea.startswith(":"):
                    print(linea, flush=True)
                continue
            datos = json.loads(linea[6:])
            if evento == "status":
                fase = datos["fase"] + (f" {datos['herramienta']}" if datos.get("herramienta") else "")
                if fase != ultima_fase:
                    print(f"\033[90m[{fase}]{RESET}", flush=True)
                    ultima_fase = fase
            elif evento == "token":
                print(datos["texto"], end="", flush=True)
            elif evento == "passthrough":
                print(datos["texto"], flush=True)
            elif evento == "error":
                print(f"\n\033[91m[error] {datos.get('detalle')}{RESET}", flush=True)
            elif evento == "done":
                print(f"\n\nsession_id: {datos.get('session_id')}   traza: {datos.get('traza_id')}")
                print(f'Para seguir la conversación: make preguntar P="..." SESION={datos.get("session_id")}',
                      flush=True)


def ayuda() -> None:
    """Imprime los objetivos del Makefile a partir de sus comentarios `## `."""
    import re

    print("\nUso: make <comando>")
    for linea in (RAIZ / "Makefile").read_text(encoding="utf-8").splitlines():
        if m := re.match(r"^## --- (.+?) -+$", linea):
            print(f"\n\033[1m{m.group(1)}{RESET}")
        elif m := re.match(r"^([a-z-]+):.*?## (.+)$", linea):
            print(f"  \033[36m{m.group(1):<20}{RESET} {m.group(2)}")
    print()


class SinColores:
    """Envuelve stdout/stderr quitando los códigos de color cuando no van a un terminal."""

    def __init__(self, flujo) -> None:
        import re

        self._flujo, self._ansi = flujo, re.compile(r"\033\[[0-9;]*m")

    def write(self, texto: str) -> int:
        return self._flujo.write(self._ansi.sub("", texto))

    def __getattr__(self, nombre: str):
        return getattr(self._flujo, nombre)


def main(nombres: list[str]) -> None:
    if WINDOWS:
        os.system("")  # activa los colores ANSI en la consola de Windows
    if not sys.stdout.isatty() or os.environ.get("NO_COLOR"):
        sys.stdout, sys.stderr = SinColores(sys.stdout), SinColores(sys.stderr)
    for flujo in (sys.stdout, sys.stderr):
        if hasattr(flujo, "reconfigure"):
            flujo.reconfigure(encoding="utf-8", errors="replace")
    if nombres == ["ayuda"]:
        ayuda()
        return
    if nombres == ["preguntar"]:
        preguntar()
        return

    definidos = definir_servicios(set(nombres))
    contenedores = ("db", "phoenix")
    desconocidos = [n for n in nombres if n not in contenedores and n not in definidos]
    if desconocidos or not nombres:
        fallo(f"servicios desconocidos {desconocidos}. "
              f"Válidos: {', '.join(contenedores)}, {', '.join(definidos)}")

    # Orden de arranque fijo (el de la definición), que ya respeta las dependencias.
    pedidos = [s for s in definidos.values() if s.nombre in nombres]
    comprobar(pedidos)
    if "agente" in nombres:
        comprobar_aws()
    if "db" in nombres:
        levantar_db()
    if "phoenix" in nombres:
        levantar_phoenix()
    if not pedidos:
        if "phoenix" in nombres:
            aviso(f"Phoenix listo en {URL_PHOENIX} (se para con make stop)")
        return

    sup = Supervisor(con_contenedores=any(n in nombres for n in contenedores))
    error = False
    try:
        for i, s in enumerate(pedidos):
            for dep in s.depende_de:
                aviso(f"{s.nombre} espera a {dep}...")
                sup.esperar(definidos[dep])
            aviso(f"Arrancando {s.nombre} en el puerto {s.puerto}")
            sup.arrancar(s, COLORES[i % len(COLORES)])
        for s in pedidos:
            sup.esperar(s)

        print()
        aviso("Todo listo. Ctrl+C para parar.")
        for s in pedidos:
            print(f"   {s.nombre:<12} http://localhost:{s.puerto}")
        if "phoenix" in nombres:
            print(f"   {'phoenix':<12} {URL_PHOENIX}")
        if "web" in nombres:
            print("\n   \033[1mAbre http://localhost:5173 en el navegador\033[0m")
        print(flush=True)

        while (nombre := sup.caido()) is None:
            time.sleep(1)
        aviso(f"{nombre} se ha parado: paro el resto")
        error = True
    except KeyboardInterrupt:
        print()
        aviso("Parando...")
    except RuntimeError as e:
        print(f"\033[91mERROR: {e}{RESET}", file=sys.stderr, flush=True)
        error = True
    finally:
        sup.parar_todo()
    if error:
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
